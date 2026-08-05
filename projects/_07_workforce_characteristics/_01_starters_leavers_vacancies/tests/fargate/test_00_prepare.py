from unittest.mock import Mock, patch

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare"


class TestPrepare:
    CLEANED_ASCWDS_WORKPLACE_SOURCE = "some/source"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pUtils.relabel_job_role_columns")
    @patch(f"{PATCH_PATH}.pUtils.pivot_job_role_cols_to_rows")
    @patch(f"{PATCH_PATH}.cUtils.merge_job_role_columns")
    @patch(f"{PATCH_PATH}.earliest_file_per_month_filter_expr")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        earliest_file_per_month_filter_expr_mock: Mock,
        merge_job_role_columns_mock: Mock,
        pivot_job_role_cols_to_rows_mock: Mock,
        relabel_job_role_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKPLACE_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_ASCWDS_WORKPLACE_SOURCE)

        reduced_data_filter_expr_mock.assert_called_once_with(
            date_col=AWPClean.ascwds_workplace_import_date
        )
        earliest_file_per_month_filter_expr_mock.assert_called_once_with(
            date_col=AWPClean.ascwds_workplace_import_date
        )

        # The retention filter must run before the monthly reduction (cheap predicate
        # first, so it can discard rows before the more expensive per-month-min window
        # runs), and both must hang off the scan chain itself, otherwise the predicates
        # are not pushed down to the parquet source and the full dataset is read first.
        scan_lf = scan_parquet_mock.return_value
        scan_lf.filter.assert_called_once_with(
            reduced_data_filter_expr_mock.return_value
        )
        retention_filtered_lf = scan_lf.filter.return_value
        retention_filtered_lf.filter.assert_called_once_with(
            earliest_file_per_month_filter_expr_mock.return_value
        )

        merge_job_role_columns_mock.assert_called_once()
        merged_jr_cols_lf = merge_job_role_columns_mock.return_value
        merged_jr_cols_lf.drop.assert_called_once()
        dropped_cols_lf = merged_jr_cols_lf.drop.return_value
        # pivot_job_role_cols_to_rows_mock.assert_called_once()

        relabel_job_role_columns_mock.assert_called_once_with(dropped_cols_lf)
        relabelled_lf = relabel_job_role_columns_mock.return_value

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=relabelled_lf,
            output_path=self.PREPARED_DATA_DESTINATION,
        )
