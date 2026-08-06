from unittest.mock import ANY, Mock, patch

import polars.selectors as cs

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate._00_prepare"


class TestPrepare:
    CLEANED_ASCWDS_WORKPLACE_SOURCE = "some/source"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.apply_categorical_labels")
    @patch(f"{PATCH_PATH}.pUtils.convert_job_role_strings_to_number_only")
    @patch(f"{PATCH_PATH}.pUtils.pivot_job_role_cols_to_rows")
    @patch(f"{PATCH_PATH}.pUtils.reduce_to_published_roles")
    @patch(f"{PATCH_PATH}.earliest_file_per_month_filter_expr")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        earliest_file_per_month_filter_expr_mock: Mock,
        reduce_to_published_roles_mock: Mock,
        pivot_job_role_cols_to_rows_mock: Mock,
        convert_job_role_strings_to_number_only_mock: Mock,
        apply_categorical_labels_mock: Mock,
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
        month_filtered_lf = retention_filtered_lf.filter.return_value

        # The job-role totals columns (28-32) are dropped before reduce_to_published_roles
        # runs, since they aren't real job role codes and would otherwise fail its
        # uncatalogued-code check. Polars selectors overload `==` to build a new
        # expression rather than compare equal/unequal, so `assert_called_once_with`
        # can't be used directly here (it raises on the ambiguous-truth-value check) -
        # compare reprs instead.
        month_filtered_lf.drop.assert_called_once()
        actual_drop_selector = month_filtered_lf.drop.call_args.args[0]
        assert repr(actual_drop_selector) == repr(cs.matches(r"^jr(28|29|30|31|32)"))
        dropped_totals_lf = month_filtered_lf.drop.return_value

        # TODO: Uncomment these assertions when the placeholder functions are implemented
        reduce_to_published_roles_mock.assert_called_once_with(dropped_totals_lf)
        merged_jr_cols_lf = reduce_to_published_roles_mock.return_value
        # pivot_job_role_cols_to_rows_mock.assert_called_once()
        # convert_job_role_strings_to_number_only_mock.assert_called_once()
        # apply_categorical_labels_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=merged_jr_cols_lf,
            output_path=self.PREPARED_DATA_DESTINATION,
        )
