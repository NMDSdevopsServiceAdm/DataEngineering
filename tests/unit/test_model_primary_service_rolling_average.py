import unittest
import warnings

import utils.estimate_filled_posts.models.primary_service_rolling_average as job
from tests.test_file_data import ModelPrimaryServiceRollingAverage as Data
from tests.test_file_schemas import ModelPrimaryServiceRollingAverage as Schemas
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils import utils


class ModelPrimaryServiceRollingAverageTests(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainModelTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days = 88
        self.estimates_df = self.spark.createDataFrame(
            Data.primary_service_rolling_average_rows,
            Schemas.primary_service_rolling_average_schema,
        )
        self.returned_df = job.model_primary_service_rolling_average(
            self.estimates_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            number_of_days,
            IndCqc.rolling_average_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rolling_average_rows,
            Schemas.expected_primary_service_rolling_average_schema,
        )
        self.returned_row_object = (
            self.returned_df.select(
                IndCqc.location_id,
                IndCqc.care_home,
                IndCqc.unix_time,
                IndCqc.ascwds_filled_posts_dedup_clean,
                IndCqc.primary_service_type,
                IndCqc.rolling_average_model,
            )
            .sort(IndCqc.location_id)
            .collect()
        )
        self.expected_row_object = self.expected_df.sort(IndCqc.location_id).collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_only_one_additional_column_returned(self):
        self.assertEqual(
            len(self.estimates_df.columns) + 1, len(self.returned_df.columns)
        )
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
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


class CalculateRollingSumTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self):
        super().setUp()
        number_of_days = 88
        self.rolling_sum_df = self.spark.createDataFrame(
            Data.rolling_sum_rows, Schemas.rolling_sum_schema
        )
        self.returned_rolling_sum_df = job.calculate_rolling_sum(
            self.rolling_sum_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            number_of_days,
        )
        self.expected_rolling_sum_df = self.spark.createDataFrame(
            Data.expected_rolling_sum_rows, Schemas.expected_rolling_sum_schema
        )

    def test_calculate_rolling_sum_does_not_add_any_rows(self):
        self.assertEqual(
            self.returned_rolling_sum_df.count(), self.rolling_sum_df.count()
        )

    def test_only_one_additional_column_returned(self):
        self.assertEqual(
            len(self.rolling_sum_df.columns) + 1,
            len(self.returned_rolling_sum_df.columns),
        )
        self.assertEqual(
            sorted(self.returned_rolling_sum_df.columns),
            sorted(self.expected_rolling_sum_df.columns),
        )

    def test_calculate_rolling_sum(self):
        self.assertEqual(
            self.returned_rolling_sum_df.collect(),
            self.expected_rolling_sum_df.collect(),
        )


class CalculateRollingCountTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self):
        super().setUp()
        number_of_days = 88
        self.rolling_count_df = self.spark.createDataFrame(
            Data.rolling_count_rows, Schemas.rolling_count_schema
        )
        self.returned_rolling_count_df = job.calculate_rolling_count(
            self.rolling_count_df,
            IndCqc.ascwds_filled_posts_dedup_clean,
            number_of_days,
        )
        self.expected_rolling_count_df = self.spark.createDataFrame(
            Data.expected_rolling_count_rows, Schemas.expected_rolling_count_schema
        )

    def test_calculate_rolling_count_does_not_add_any_rows(self):
        self.assertEqual(
            self.returned_rolling_count_df.count(), self.rolling_count_df.count()
        )

    def test_only_one_additional_column_returned(self):
        self.assertEqual(
            len(self.rolling_count_df.columns) + 1,
            len(self.returned_rolling_count_df.columns),
        )
        self.assertEqual(
            sorted(self.returned_rolling_count_df.columns),
            sorted(self.expected_rolling_count_df.columns),
        )

    def test_calculate_rolling_count(self):
        self.assertEqual(
            self.returned_rolling_count_df.collect(),
            self.expected_rolling_count_df.collect(),
        )


class CalculateRollingAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self):
        super().setUp()
        self.rolling_average_df = self.spark.createDataFrame(
            Data.rolling_average_rows, Schemas.rolling_average_schema
        )
        self.returned_rolling_average_df = job.calculate_rolling_average(
            self.rolling_average_df,
            IndCqc.rolling_average_model,
        )
        self.expected_rolling_average_df = self.spark.createDataFrame(
            Data.expected_rolling_average_rows, Schemas.expected_rolling_average_schema
        )

    def test_calculate_rolling_average_does_not_add_any_rows(self):
        self.assertEqual(
            self.returned_rolling_average_df.count(), self.rolling_average_df.count()
        )

    def test_only_one_additional_column_returned(self):
        self.assertEqual(
            len(self.rolling_average_df.columns) + 1,
            len(self.returned_rolling_average_df.columns),
        )
        self.assertEqual(
            sorted(self.returned_rolling_average_df.columns),
            sorted(self.expected_rolling_average_df.columns),
        )

    def test_calculate_rolling_average(self):
        self.assertEqual(
            self.returned_rolling_average_df.collect(),
            self.expected_rolling_average_df.collect(),
        )
