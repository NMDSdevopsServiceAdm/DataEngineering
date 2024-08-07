import unittest
import warnings

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
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class RollingAverageModelTests(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.returned_df = job.model_primary_service_rolling_average(
            self.estimates_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            88,
            IndCqc.rolling_average_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_rolling_average_rows, Schemas.expected_rolling_average_schema
        )
        self.returned_row_object = (
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
        self.expected_row_object = self.expected_df.collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_only_one_additional_column_returned(self):
        self.assertEqual(
            len(self.estimates_df.columns) + 1, len(self.returned_df.columns)
        )
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.expected_df.columns)
        )

    def test_returned_rolling_average_model_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_row_object)):
            self.assertEqual(
                self.returned_row_object[i][IndCqc.rolling_average_model],
                self.expected_row_object[i][IndCqc.rolling_average_model],
                f"Returned row {i} does not match expected",
            )


class CreateRollingAverageColumn(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.calculate_rolling_average_column_rows,
            Schemas.calculate_rolling_average_column_schema,
        )
        self.returned_df = job.create_rolling_average_column(self.test_df, 88)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rolling_average_column_rows,
            Schemas.expected_calculate_rolling_average_column_schema,
        )

    def test_create_rolling_average_column_does_not_add_any_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_create_rolling_average_column_returns_correct_values(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class CalculateRollingSum(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.rolling_sum_rows, Schemas.rolling_sum_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_rolling_sum_rows, Schemas.expected_rolling_sum_schema
        )
        self.returned_df = job.calculate_rolling_sum(
            self.test_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            88,
            IndCqc.rolling_sum,
        )

    def test_calculate_rolling_sum_does_not_add_any_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_df.count())

    def test_calculate_rolling_sum(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())


class AddFlagIfIncludedInCount(TestModelPrimaryServiceRollingAverage):
    def setUp(self):
        super().setUp()
        self.test_df = self.spark.createDataFrame(
            Data.add_flag_rows, Schemas.add_flag_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_flag_rows, Schemas.expected_add_flag_schema
        )
        self.returned_df = job.add_flag_if_included_in_count(self.test_df)

    def test_add_flag_if_included_in_count(self):
        self.assertEqual(self.returned_df.collect(), self.expected_df.collect())
