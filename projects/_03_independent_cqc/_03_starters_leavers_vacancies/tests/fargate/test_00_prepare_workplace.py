from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import polars.selectors as cs
from polars.testing import assert_frame_equal

import projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._00_prepare_workplace as job
from projects._03_independent_cqc._03_starters_leavers_vacancies.unittest_data.polars_slv_test_data import (
    TestPrepareMainData as Data,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._03_starters_leavers_vacancies.fargate._00_prepare_workplace"


class TestMain:
    CLEANED_ASCWDS_WORKPLACE_SOURCE = "some/source"
    METADATA_SOURCE = "some/metadata"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pWorkplaceUtils.relabel_job_role_columns")
    @patch(f"{PATCH_PATH}.pWorkplaceUtils.reshape_job_role_cols_to_rows")
    @patch(f"{PATCH_PATH}.pWorkplaceUtils.reduce_to_published_roles")
    @patch(f"{PATCH_PATH}.get_matched_ascwds_dates")
    @patch(f"{PATCH_PATH}.not_null_filter_expr")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_filters_workplace_data_to_dates_present_in_metadata(
        self,
        scan_parquet_mock: Mock,
        not_null_filter_expr_mock: Mock,
        get_matched_ascwds_dates_mock: Mock,
        reduce_to_published_roles_mock: Mock,
        reshape_job_role_cols_to_rows_mock: Mock,
        relabel_job_role_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKPLACE_SOURCE,
            self.METADATA_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_ASCWDS_WORKPLACE_SOURCE)

        not_null_filter_expr_mock.assert_called_once_with(column=AWPClean.location_id)
        get_matched_ascwds_dates_mock.assert_called_once_with(
            self.METADATA_SOURCE, IndCQC.ascwds_workplace_import_date
        )

        # The null-location filter runs first (cheapest predicate), then the
        # metadata-matched-dates filter, and both must hang off the scan chain itself,
        # otherwise the predicates are not pushed down to the parquet source and the
        # full dataset is read first.
        scan_lf = scan_parquet_mock.return_value
        scan_lf.filter.assert_called_once_with(not_null_filter_expr_mock.return_value)
        location_filtered_lf = scan_lf.filter.return_value
        location_filtered_lf.filter.assert_called_once()
        is_in_expr = location_filtered_lf.filter.call_args.args[0]
        date_filtered_lf = location_filtered_lf.filter.return_value

        # The is_in() expression is built from the metadata-matched dates rather than
        # the old two-filter chain - Polars expressions overload `==` to build a new
        # expression rather than compare equal/unequal, so structural equality is
        # checked via `.meta.eq()` instead.
        expected_is_in_expr = pl.col(AWPClean.ascwds_workplace_import_date).is_in(
            get_matched_ascwds_dates_mock.return_value.implode.return_value
        )
        assert is_in_expr.meta.eq(expected_is_in_expr)

        # The job-role totals columns (28-32) are dropped before reduce_to_published_roles
        # runs, since they aren't real job role codes and would otherwise fail its
        # uncatalogued-code check. Polars selectors overload `==` to build a new
        # expression rather than compare equal/unequal, so `assert_called_once_with`
        # can't be used directly here (it raises on the ambiguous-truth-value check) -
        # compare reprs instead.
        date_filtered_lf.drop.assert_called_once()
        actual_drop_selector = date_filtered_lf.drop.call_args.args[0]
        assert repr(actual_drop_selector) == repr(cs.matches(r"^jr(28|29|30|31|32)"))
        dropped_totals_lf = date_filtered_lf.drop.return_value

        reduce_to_published_roles_mock.assert_called_once_with(dropped_totals_lf)
        merged_jr_cols_lf = reduce_to_published_roles_mock.return_value

        relabel_job_role_columns_mock.assert_called_once_with(merged_jr_cols_lf)
        relabelled_lf = relabel_job_role_columns_mock.return_value

        reshape_job_role_cols_to_rows_mock.assert_called_once_with(relabelled_lf)
        reshaped_lf = reshape_job_role_cols_to_rows_mock.return_value

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=reshaped_lf,
            output_path=self.PREPARED_DATA_DESTINATION,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pWorkplaceUtils.relabel_job_role_columns")
    @patch(f"{PATCH_PATH}.pWorkplaceUtils.reshape_job_role_cols_to_rows")
    @patch(f"{PATCH_PATH}.pWorkplaceUtils.reduce_to_published_roles")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_keeps_late_arriving_file_when_metadata_matched_to_it(
        self,
        scan_parquet_mock: Mock,
        reduce_to_published_roles_mock: Mock,
        reshape_job_role_cols_to_rows_mock: Mock,
        relabel_job_role_columns_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        # Metadata is only CQC-matched to 2024-10-08. The cleaned source also carries
        # 2024-10-01 - the date the old hardcoded quarterly/earliest-file-per-month
        # rule would have kept instead of the later, metadata-matched file. Filtering
        # and get_matched_ascwds_dates are left unmocked here so the real join between
        # the two runs, proving the fix rather than just the mocked call shape.
        cleaned_workplace_lf = pl.LazyFrame(
            Data.cleaned_workplace_with_late_arriving_file_data
        )
        metadata_lf = pl.LazyFrame(Data.metadata_matched_to_late_arriving_file_data)
        scan_parquet_mock.side_effect = [cleaned_workplace_lf, metadata_lf]

        job.main(
            self.CLEANED_ASCWDS_WORKPLACE_SOURCE,
            self.METADATA_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        filtered_lf = reduce_to_published_roles_mock.call_args.args[0]
        expected_lf = pl.LazyFrame(
            {
                AWPClean.location_id: ["loc1"],
                AWPClean.establishment_id: ["1-001"],
                AWPClean.ascwds_workplace_import_date: [date(2024, 10, 8)],
            }
        )
        assert_frame_equal(filtered_lf.collect(), expected_lf.collect())
