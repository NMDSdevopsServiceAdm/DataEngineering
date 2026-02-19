import warnings
from unittest.mock import Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change_trendline as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ModelPrimaryServiceRateOfChangeTrendlineData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ModelPrimaryServiceRateOfChangeTrendlineSchemas as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change_trendline"


class ModelPrimaryServiceRateOfChangeTrendlineTests(SparkBaseTest):
    def setUp(self):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)


class MainTests(ModelPrimaryServiceRateOfChangeTrendlineTests):
    def setUp(self) -> None:
        super().setUp()

        self.number_of_days: int = 3
        self.input_df = self.spark.createDataFrame(
            Data.primary_service_rate_of_change_trendline_rows,
            Schemas.primary_service_rate_of_change_trendline_schema,
        )
        self.returned_df = job.model_primary_service_rate_of_change_trendline(
            self.input_df,
            IndCqc.combined_ratio_and_filled_posts,
            self.number_of_days,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_primary_service_rate_of_change_trendline_rows,
            Schemas.expected_primary_service_rate_of_change_trendline_schema,
        )
        self.returned_data = self.returned_df.sort(
            IndCqc.location_id, IndCqc.unix_time
        ).collect()
        self.expected_data = self.expected_df.collect()

    @patch(f"{PATCH_PATH}.calculate_rate_of_change_trendline")
    @patch(f"{PATCH_PATH}.model_primary_service_rate_of_change")
    @patch(f"{PATCH_PATH}.cUtils.create_banded_bed_count_column")
    def test_main_calls_functions(
        self,
        create_banded_bed_count_column_mock: Mock,
        model_primary_service_rate_of_change_mock: Mock,
        calculate_roc_trendline_mock: Mock,
    ):
        calculate_roc_trendline_mock.return_value = Mock(name="roc_trendline_df")

        job.model_primary_service_rate_of_change_trendline(
            self.input_df,
            IndCqc.combined_ratio_and_filled_posts,
            self.number_of_days,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )

        create_banded_bed_count_column_mock.assert_called_once()
        model_primary_service_rate_of_change_mock.assert_called_once()
        calculate_roc_trendline_mock.assert_called_once()

    def test_row_count_unchanged_after_running_full_job(self):
        self.assertEqual(self.input_df.count(), self.returned_df.count())

    def test_returned_rate_of_change_trendline_values_match_expected(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                self.expected_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                places=3,
                msg=f"Returned row {i} does not match expected",
            )

    def test_expected_columns_are_returned(self):
        self.assertEqual(
            sorted(self.returned_df.columns),
            sorted(self.expected_df.columns),
        )


class CalculateRateOfChangeTrendlineTests(
    ModelPrimaryServiceRateOfChangeTrendlineTests
):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.calculate_rate_of_change_trendline_rows,
            Schemas.calculate_rate_of_change_trendline_schema,
        )
        self.returned_df = job.calculate_rate_of_change_trendline(
            test_df,
            IndCqc.ascwds_rate_of_change_trendline_model,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_rate_of_change_trendline_rows,
            Schemas.expected_calculate_rate_of_change_trendline_schema,
        )

        self.returned_data = self.returned_df.sort(
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_roc,
            IndCqc.unix_time,
        ).collect()
        self.expected_data = self.expected_df.collect()

    def test_returned_column_names_match_expected(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_returns_correct_rate_of_change_trendline_values(self):
        for i in range(len(self.returned_data)):
            self.assertAlmostEqual(
                self.returned_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                self.expected_data[i][IndCqc.ascwds_rate_of_change_trendline_model],
                places=2,
                msg=f"Returned row {i} does not match expected",
            )
