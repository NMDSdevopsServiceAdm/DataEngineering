import unittest
from unittest.mock import patch, Mock

import jobs.diagnostics_on_capacity_tracker as job
from tests.test_file_data import DiagnosticsOnCapacityTrackerData as Data
from tests.test_file_schemas import DiagnosticsOnCapacityTrackerSchemas as Schemas
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)

PATCH_PATH: str = "jobs.diagnostics_on_capacity_tracker"


class DiagnosticsOnCapacityTrackerTests(unittest.TestCase):
    ESTIMATED_FILLED_POSTS_SOURCE = "some/directory"
    CARE_HOME_DIAGNOSTICS_DESTINATION = "some/other/directory"
    CARE_HOME_SUMMARY_DIAGNOSTICS_DESTINATION = "another/directory"
    NON_RES_DIAGNOSTICS_DESTINATION = "some/other/directory"
    NON_RES_SUMMARY_DIAGNOSTICS_DESTINATION = "yet/another/directory"
    partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    def setUp(self):
        self.spark = utils.get_spark()
        self.estimate_jobs_df = self.spark.createDataFrame(
            Data.estimate_filled_posts_rows,
            Schemas.estimate_filled_posts_schema,
        )


class MainTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.dUtils.create_summary_diagnostics_table")
    @patch(f"{PATCH_PATH}.run_diagnostics_for_non_residential")
    @patch(f"{PATCH_PATH}.run_diagnostics_for_care_homes")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main_runs(
        self,
        read_from_parquet_mock: Mock,
        run_diagnostics_for_care_homes_mock: Mock,
        run_diagnostics_for_non_residential_mock: Mock,
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
        run_diagnostics_for_care_homes_mock.assert_called_once()
        run_diagnostics_for_non_residential_mock.assert_called_once()
        self.assertEqual(create_summary_diagnostics_table_mock.call_count, 2)
        self.assertEqual(write_to_parquet_mock.call_count, 4)


class RunDiagnosticsForCareHomes(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.dUtils.calculate_aggregate_residuals")
    @patch(f"{PATCH_PATH}.dUtils.calculate_residuals")
    @patch(f"{PATCH_PATH}.dUtils.calculate_distribution_metrics")
    @patch(f"{PATCH_PATH}.dUtils.create_window_for_model_and_service_splits")
    @patch(f"{PATCH_PATH}.dUtils.filter_to_known_values")
    @patch(f"{PATCH_PATH}.dUtils.restructure_dataframe_to_column_wise")
    @patch(f"{PATCH_PATH}.dUtils.create_list_of_models")
    @patch(f"{PATCH_PATH}.model_imputation_with_extrapolation_and_interpolation")
    @patch(f"{PATCH_PATH}.model_primary_service_rate_of_change_trendline")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_value")
    def test_run_diagnostics_for_care_homes_runs(
        self,
        select_rows_with_value_mock: Mock,
        model_primary_service_rate_of_change_trendline_mock: Mock,
        model_imputation_with_extrapolation_and_interpolation_mock: Mock,
        create_list_of_models_mock: Mock,
        restructure_dataframe_to_column_wise_mock: Mock,
        filter_to_known_values_mock: Mock,
        create_window_for_model_and_service_splits_mock: Mock,
        calculate_distribution_metrics_mock: Mock,
        calculate_residuals_mock: Mock,
        calculate_aggregate_residuals_mock: Mock,
    ):
        job.run_diagnostics_for_care_homes(self.estimate_jobs_df)

        select_rows_with_value_mock.assert_called_once()
        model_primary_service_rate_of_change_trendline_mock.assert_called_once()
        model_imputation_with_extrapolation_and_interpolation_mock.assert_called_once()
        create_list_of_models_mock.assert_called_once()
        restructure_dataframe_to_column_wise_mock.assert_called_once()
        filter_to_known_values_mock.assert_called_once()
        create_window_for_model_and_service_splits_mock.assert_called_once()
        calculate_distribution_metrics_mock.assert_called_once()
        calculate_residuals_mock.assert_called_once()
        calculate_aggregate_residuals_mock.assert_called_once()


class RunDiagnosticsForNonResidential(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.dUtils.calculate_aggregate_residuals")
    @patch(f"{PATCH_PATH}.dUtils.calculate_residuals")
    @patch(f"{PATCH_PATH}.dUtils.calculate_distribution_metrics")
    @patch(f"{PATCH_PATH}.dUtils.create_window_for_model_and_service_splits")
    @patch(f"{PATCH_PATH}.dUtils.filter_to_known_values")
    @patch(f"{PATCH_PATH}.dUtils.restructure_dataframe_to_column_wise")
    @patch(f"{PATCH_PATH}.dUtils.create_list_of_models")
    @patch(f"{PATCH_PATH}.merge_columns_in_order")
    @patch(f"{PATCH_PATH}.convert_to_all_posts_using_ratio")
    @patch(f"{PATCH_PATH}.calculate_care_worker_ratio")
    @patch(f"{PATCH_PATH}.model_imputation_with_extrapolation_and_interpolation")
    @patch(f"{PATCH_PATH}.model_primary_service_rate_of_change_trendline")
    @patch(f"{PATCH_PATH}.utils.select_rows_with_value")
    def test_run_diagnostics_for_non_residential_runs(
        self,
        select_rows_with_value_mock: Mock,
        model_primary_service_rate_of_change_trendline_mock: Mock,
        model_imputation_with_extrapolation_and_interpolation_mock: Mock,
        calculate_care_worker_ratio_mock: Mock,
        convert_to_all_posts_using_ratio_mock: Mock,
        merge_columns_in_order_mock: Mock,
        create_list_of_models_mock: Mock,
        restructure_dataframe_to_column_wise_mock: Mock,
        filter_to_known_values_mock: Mock,
        create_window_for_model_and_service_splits_mock: Mock,
        calculate_distribution_metrics_mock: Mock,
        calculate_residuals_mock: Mock,
        calculate_aggregate_residuals_mock: Mock,
    ):
        job.run_diagnostics_for_non_residential(self.estimate_jobs_df)

        select_rows_with_value_mock.assert_called_once()
        model_primary_service_rate_of_change_trendline_mock.assert_called_once()
        model_imputation_with_extrapolation_and_interpolation_mock.assert_called_once()
        calculate_care_worker_ratio_mock.assert_called_once()
        convert_to_all_posts_using_ratio_mock.assert_called_once()
        merge_columns_in_order_mock.assert_called_once()
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

    def test_number_of_days_in_window_is_expected_value(self):
        self.assertEqual(job.number_of_days_in_window, 95)
        self.assertIsInstance(job.number_of_days_in_window, int)

    def test_max_number_of_days_to_interpolate_between_is_expected_value(self):
        self.assertEqual(job.max_number_of_days_to_interpolate_between, 185)
        self.assertIsInstance(job.max_number_of_days_to_interpolate_between, int)


class CalculateCareWorkerRatioTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_care_worker_ratio_returns_correct_ratio(self):
        test_df = self.spark.createDataFrame(
            Data.calculate_care_worker_ratio_rows, Schemas.calculate_care_worker_schema
        )
        expected_ratio = Data.expected_care_worker_ratio
        returned_ratio = job.calculate_care_worker_ratio(test_df)
        self.assertEqual(returned_ratio, expected_ratio)


class ConvertToAllPostsUsingRatioTests(DiagnosticsOnCapacityTrackerTests):
    def setUp(self) -> None:
        super().setUp()

    def test_convert_to_all_posts_using_ratio_returns_correct_values(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.convert_to_all_posts_using_ratio_rows,
            Schemas.convert_to_all_posts_using_ratio_schema,
        )
        test_ratio = Data.expected_care_worker_ratio
        expected_df = self.spark.createDataFrame(
            Data.expected_convert_to_all_posts_using_ratio_rows,
            Schemas.expected_convert_to_all_posts_using_ratio_schema,
        )
        returned_df = job.convert_to_all_posts_using_ratio(test_df, test_ratio)

        self.assertEquals(
            returned_df.sort(IndCQC.location_id).collect(),
            expected_df.collect(),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
