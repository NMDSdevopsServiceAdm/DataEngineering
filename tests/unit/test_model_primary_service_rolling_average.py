import unittest
import warnings
import pyspark.sql.functions as F


import utils.estimate_filled_posts.models.primary_service_rolling_average as job
from tests.test_file_data import ModelPrimaryServiceRollingAverage as Data
from tests.test_file_schemas import ModelPrimaryServiceRollingAverage as Schemas
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils import utils


class TestModelPrimaryServiceRollingAverage(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()
        self.estimates_df = self.spark.createDataFrame(
            Data.input_rows, Schemas.input_schema
        )
        self.known_filled_posts_df = self.spark.createDataFrame(
            Data.known_filled_posts_rows, Schemas.known_filled_posts_schema
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class RollingAverageModelTests(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.returned_df = job.model_primary_service_rolling_average(
            self.estimates_df, 88
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_rolling_average_rows, Schemas.expected_rolling_average_schema
        )

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_model_primary_service_rolling_averages_are_correct(self):
        returned_df = (
            self.returned_df.select(
                IndCqc.location_id,
                IndCqc.cqc_location_import_date,
                IndCqc.unix_time,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.primary_service_type,
                IndCqc.rolling_average_model,
            )
            .sort(IndCqc.location_id)
            .collect()
        )
        expected_df = self.expected_df.collect()
        self.returned_df.show()
        self.expected_df.show()

        self.assertEqual(returned_df, expected_df)


class FilterToLocationsWithKnownFilledPostsTest(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.estimates_df = self.spark.createDataFrame(
            Data.input_rows, Schemas.input_schema
        )

    def test_filter_to_locations_with_known_filled_posts(self):
        df = job.filter_to_locations_with_known_filled_posts(self.estimates_df)
        self.assertEqual(df.count(), 10)
        self.assertEqual(
            df.where(F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNull()).count(), 0
        )


class CalculateFilledPostsAggregates(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()

    def test_calculate_filled_posts_aggregates_per_service_and_time_period(self):
        df = job.calculate_filled_posts_aggregates_per_service_and_time_period(
            self.known_filled_posts_df
        )
        self.assertEqual(df.count(), 8)
        df = df.sort(
            F.col(IndCqc.primary_service_type).desc(), F.col(IndCqc.unix_time).asc()
        ).collect()
        self.assertEqual(df[0][IndCqc.count_of_filled_posts], 2)
        self.assertEqual(df[0][IndCqc.sum_of_filled_posts], 10.0)
        self.assertEqual(df[7][IndCqc.count_of_filled_posts], 1)
        self.assertEqual(df[7][IndCqc.sum_of_filled_posts], 142.0)


class CreateRollingAverageColumn(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.data_for_rolling_avg = self.spark.createDataFrame(
            Data.calculate_rolling_average_column_rows,
            Schemas.calculate_rolling_average_column_schema,
        )

    def test_create_rolling_average_column(self):
        df = job.create_rolling_average_column(self.data_for_rolling_avg, 88)
        self.assertEqual(df.count(), 8)
        df = df.collect()
        self.assertEqual(df[0][IndCqc.rolling_average_model], 15.0)
        self.assertEqual(df[2][IndCqc.rolling_average_model], 70.25)
        self.assertEqual(df[4][IndCqc.rolling_average_model], 5.0)
        self.assertEqual(df[6][IndCqc.rolling_average_model], 15.0)


class CalculateRollingSum(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.rolling_sum_df = self.spark.createDataFrame(
            Data.rolling_sum_rows, Schemas.rolling_sum_schema
        )

    def test_calculate_rolling_sum(self):
        df = job.calculate_rolling_sum(
            self.rolling_sum_df, "col_to_sum", 3, "rolling_total"
        )
        self.assertEqual(df.count(), 7)
        df = df.collect()
        self.assertEqual(df[0]["rolling_total"], 10.0)
        self.assertEqual(df[3]["rolling_total"], 54.0)
        self.assertEqual(df[4]["rolling_total"], 64.0)
        self.assertEqual(df[6]["rolling_total"], 21.0)


class JoinRollingAverage(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.rolling_avg_df = self.spark.createDataFrame(
            Data.rolling_average_rows, Schemas.rolling_average_schema
        )

    def test_join_rolling_average_into_df(self):
        main_df = self.known_filled_posts_df
        rolling_avg_df = self.rolling_avg_df

        df = job.join_rolling_average_into_df(main_df, rolling_avg_df)
        self.assertEqual(df.count(), 10)
        self.assertEqual(len(df.columns), len(main_df.columns) + 1)
        df = df.collect()
        self.assertEqual(df[1][IndCqc.rolling_average_model], 44.24)
        self.assertEqual(df[9][IndCqc.rolling_average_model], 25.1)
