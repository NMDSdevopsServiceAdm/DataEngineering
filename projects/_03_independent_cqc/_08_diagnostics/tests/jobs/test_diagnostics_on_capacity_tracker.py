from unittest.mock import Mock, patch

import projects._03_independent_cqc._08_diagnostics.jobs.diagnostics_on_capacity_tracker as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    DiagnosticsOnCapacityTrackerData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    DiagnosticsOnCapacityTrackerSchemas as Schemas,
)
from tests.base_test import SparkBaseTest
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

PATCH_PATH: str = (
    "projects._03_independent_cqc._08_diagnostics.jobs.diagnostics_on_capacity_tracker"
)


class DiagnosticsOnCapacityTrackerTests(SparkBaseTest):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    CARE_HOME_DIAGNOSTICS_DESTINATION = "some/other/directory"
    CARE_HOME_SUMMARY_DIAGNOSTICS_DESTINATION = "another/directory"
    NON_RES_DIAGNOSTICS_DESTINATION = "some/other/directory"
    NON_RES_SUMMARY_DIAGNOSTICS_DESTINATION = "yet/another/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_filled_posts_rows,
            Schemas.estimate_filled_posts_schema,
        )
        self.care_home: str = CareHome.care_home
        self.diagnostic_col: str = "some_col"


class MainTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.dUtils.create_summary_diagnostics_table")
    @patch(f"{PATCH_PATH}.run_diagnostics")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_mock: Mock,
        run_diagnostics_mock: Mock,
        create_summary_diagnostics_table_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.side_effect = self.estimate_jobs_df

        job.main(
            self.ESTIMATED_FILLED_POSTS_SOURCE,
            self.CARE_HOME_DIAGNOSTICS_DESTINATION,
            self.CARE_HOME_SUMMARY_DIAGNOSTICS_DESTINATION,
            self.NON_RES_DIAGNOSTICS_DESTINATION,
            self.NON_RES_SUMMARY_DIAGNOSTICS_DESTINATION,
        )

        read_from_parquet_mock.assert_called_once_with(
            self.ESTIMATED_FILLED_POSTS_SOURCE, job.estimate_filled_posts_columns
        )
        self.assertEqual(run_diagnostics_mock.call_count, 2)
        self.assertEqual(create_summary_diagnostics_table_mock.call_count, 2)
        self.assertEqual(write_to_parquet_mock.call_count, 4)


class RunDiagnostics(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.dUtils.calculate_aggregate_residuals")
    @patch(f"{PATCH_PATH}.dUtils.calculate_residuals")
    @patch(f"{PATCH_PATH}.dUtils.calculate_distribution_metrics")
    @patch(f"{PATCH_PATH}.dUtils.create_window_for_model_and_service_splits")
    @patch(f"{PATCH_PATH}.dUtils.filter_to_known_values")
    @patch(f"{PATCH_PATH}.dUtils.restructure_dataframe_to_column_wise")
    @patch(f"{PATCH_PATH}.dUtils.create_list_of_models")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_value")
    def test_run_diagnostics_runs(
        self,
        select_rows_with_value_mock: Mock,
        create_list_of_models_mock: Mock,
        restructure_dataframe_to_column_wise_mock: Mock,
        filter_to_known_values_mock: Mock,
        create_window_for_model_and_service_splits_mock: Mock,
        calculate_distribution_metrics_mock: Mock,
        calculate_residuals_mock: Mock,
        calculate_aggregate_residuals_mock: Mock,
    ):
        job.run_diagnostics(self.estimate_jobs_df, self.care_home, self.diagnostic_col)

        select_rows_with_value_mock.assert_called_once()
        create_list_of_models_mock.assert_called_once()
        restructure_dataframe_to_column_wise_mock.assert_called_once()
        filter_to_known_values_mock.assert_called_once()
        create_window_for_model_and_service_splits_mock.assert_called_once()
        calculate_distribution_metrics_mock.assert_called_once()
        calculate_residuals_mock.assert_called_once()
        calculate_aggregate_residuals_mock.assert_called_once()


class CheckConstantsTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    def test_absolute_value_cutoff_is_expected_value(self):
        self.assertEqual(job.absolute_value_cutoff, 10.0)
        self.assertIsInstance(job.absolute_value_cutoff, float)

    def test_percentage_value_cutoff_is_expected_value(self):
        self.assertEqual(job.percentage_value_cutoff, 0.25)
        self.assertIsInstance(job.percentage_value_cutoff, float)

    def test_standardised_value_cutoff_is_expected_value(self):
        self.assertEqual(job.standardised_value_cutoff, 1.0)
        self.assertIsInstance(job.standardised_value_cutoff, float)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
