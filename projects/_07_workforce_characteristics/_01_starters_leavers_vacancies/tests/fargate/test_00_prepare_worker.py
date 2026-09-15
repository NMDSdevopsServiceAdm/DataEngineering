from unittest.mock import Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare_worker as job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare_worker"


class TestPrepareWorker:
    CLEANED_ASCWDS_WORKER_SOURCE = "some/source"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pWorkerUtils.reshape_employment_status_data")
    @patch(f"{PATCH_PATH}.pWorkerUtils.aggregate_employment_status_data")
    @patch(f"{PATCH_PATH}.pWorkerUtils.collapse_job_roles_to_published_labels")
    @patch(f"{PATCH_PATH}.earliest_file_per_month_filter_expr")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.not_null_filter_expr")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        not_null_filter_expr_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        earliest_file_per_month_filter_expr_mock: Mock,
        collapse_job_roles_to_published_labels_mock: Mock,
        aggregate_employment_status_data_mock: Mock,
        reshape_employment_status_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKER_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_ASCWDS_WORKER_SOURCE)

        not_null_filter_expr_mock.assert_called_once_with(column=AWKClean.location_id)
        reduced_data_filter_expr_mock.assert_called_once_with(
            date_col=AWKClean.ascwds_worker_import_date
        )
        earliest_file_per_month_filter_expr_mock.assert_called_once_with(
            date_col=AWKClean.ascwds_worker_import_date
        )

        # All three filters must hang off the scan chain itself (not a separately
        # materialised frame), so the predicates are pushed down to the parquet source.
        scan_lf = scan_parquet_mock.return_value
        scan_lf.filter.assert_called_once_with(not_null_filter_expr_mock.return_value)
        location_filtered_lf = scan_lf.filter.return_value
        location_filtered_lf.filter.assert_called_once_with(
            reduced_data_filter_expr_mock.return_value
        )
        retention_filtered_lf = location_filtered_lf.filter.return_value
        retention_filtered_lf.filter.assert_called_once_with(
            earliest_file_per_month_filter_expr_mock.return_value
        )
        worker_lf = retention_filtered_lf.filter.return_value

        collapse_job_roles_to_published_labels_mock.assert_called_once_with(worker_lf)
        collapsed_lf = collapse_job_roles_to_published_labels_mock.return_value

        aggregate_employment_status_data_mock.assert_called_once_with(collapsed_lf)
        aggregated_lf = aggregate_employment_status_data_mock.return_value

        reshape_employment_status_data_mock.assert_called_once_with(aggregated_lf)
        reshaped_lf = reshape_employment_status_data_mock.return_value

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=reshaped_lf,
            output_path=self.PREPARED_DATA_DESTINATION,
        )
