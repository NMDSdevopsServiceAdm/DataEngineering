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


class MainTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days = 89
        self.estimates_df = self.spark.createDataFrame(
            Data.primary_service_rolling_average_rows,
            Schemas.primary_service_rolling_average_schema,
        )
        self.returned_df = job.model_primary_service_rolling_average(
            self.estimates_df,
            IndCqc.filled_posts_per_bed_ratio,
            IndCqc.ascwds_filled_posts_dedup_clean,
            number_of_days,
            IndCqc.rolling_average_model_filled_posts_per_bed_ratio,
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
                IndCqc.rolling_average_model_filled_posts_per_bed_ratio,
            )
            .sort(IndCqc.location_id)
            .collect()
        )
        self.expected_row_object = self.expected_df.sort(IndCqc.location_id).collect()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.estimates_df.count(), self.returned_df.count())

    def test_model_primary_service_rolling_average_returns_expected_columns(self):
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


class CreateSingleColumnToAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.single_column_to_average_rows,
            Schemas.single_column_to_average_schema,
        )
        self.returned_df = job.create_single_column_to_average(
            test_df,
            IndCqc.filled_posts_per_bed_ratio,
            IndCqc.ascwds_filled_posts_dedup_clean,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_single_column_to_average_rows,
            Schemas.expected_single_column_to_average_schema,
        )
        self.returned_data = self.returned_df.collect()
        self.expected_data = self.expected_df.sort(IndCqc.location_id).collect()

    def test_create_single_column_to_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_column_to_average_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][job.TempCol.temp_column_to_average],
                self.expected_data[i][job.TempCol.temp_column_to_average],
                f"Returned row {i} does not match expected",
            )


class CalculateRollingAverageTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        number_of_days: int = 3

        test_df = self.spark.createDataFrame(
            Data.calculate_rolling_average_rows,
            Schemas.calculate_rolling_average_schema,
        )
        self.returned_df = job.calculate_rolling_average(test_df, number_of_days)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rolling_average_rows,
            Schemas.expected_calculate_rolling_average_schema,
        )
        self.returned_data = self.returned_df.collect()
        self.expected_data = self.expected_df.sort(IndCqc.location_id).collect()

    def test_calculate_rolling_average_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_rolling_average_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][job.TempCol.temp_rolling_average],
                self.expected_data[i][job.TempCol.temp_rolling_average],
                2,
                f"Returned row {i} does not match expected",
            )


class CreateFinalModelColumnsTests(ModelPrimaryServiceRollingAverageTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.create_final_model_columns_rows,
            Schemas.create_final_model_columns_schema,
        )
        self.returned_df = job.create_final_model_columns(
            test_df,
            IndCqc.rolling_average_model_filled_posts_per_bed_ratio,
            IndCqc.rolling_average_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_final_model_columns_rows,
            Schemas.expected_create_final_model_columns_schema,
        )
        self.returned_data = self.returned_df.collect()
        self.expected_data = self.expected_df.sort(IndCqc.location_id).collect()

    def test_create_final_model_columns_returns_expected_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )

    def test_returned_model_filled_posts_per_bed_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][
                    IndCqc.rolling_average_model_filled_posts_per_bed_ratio
                ],
                self.expected_data[i][
                    IndCqc.rolling_average_model_filled_posts_per_bed_ratio
                ],
                2,
                f"Returned row {i} does not match expected",
            )

    def test_returned_model_filled_posts_values_match_expected(
        self,
    ):
        for i in range(len(self.returned_data)):
            self.assertEqual(
                self.returned_data[i][IndCqc.rolling_average_model],
                self.expected_data[i][IndCqc.rolling_average_model],
                f"Returned row {i} does not match expected",
            )
