from unittest.mock import Mock, patch

import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate._00_prepare_worker as job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = (
    "projects._03_independent_cqc._02_employment_status.fargate._00_prepare_worker"
)


class TestMain:
    CLEANED_ASCWDS_WORKER_SOURCE = "some/source"
    METADATA_SOURCE = "some/metadata"
    PREPARED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.pWorkerUtils.reshape_employment_status_data")
    @patch(f"{PATCH_PATH}.pWorkerUtils.aggregate_employment_status_data")
    @patch(f"{PATCH_PATH}.pWorkerUtils.collapse_job_roles_to_published_labels")
    @patch(f"{PATCH_PATH}.get_matched_ascwds_dates")
    @patch(f"{PATCH_PATH}.not_null_filter_expr")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_filters_worker_data_to_dates_present_in_metadata(
        self,
        scan_parquet_mock: Mock,
        not_null_filter_expr_mock: Mock,
        get_matched_ascwds_dates_mock: Mock,
        collapse_job_roles_to_published_labels_mock: Mock,
        aggregate_employment_status_data_mock: Mock,
        reshape_employment_status_data_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.CLEANED_ASCWDS_WORKER_SOURCE,
            self.METADATA_SOURCE,
            self.PREPARED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.CLEANED_ASCWDS_WORKER_SOURCE)

        not_null_filter_expr_mock.assert_called_once_with(column=AWKClean.location_id)
        # Workplace and worker files always land the same day, so the worker date
        # column is filtered against the dates matched against the workplace import
        # date in metadata rather than a separate worker-keyed lookup.
        get_matched_ascwds_dates_mock.assert_called_once_with(
            self.METADATA_SOURCE, IndCQC.ascwds_workplace_import_date
        )

        # Both filters must hang off the scan chain itself (not a separately
        # materialised frame), so the predicates are pushed down to the parquet source.
        scan_lf = scan_parquet_mock.return_value
        scan_lf.filter.assert_called_once_with(not_null_filter_expr_mock.return_value)
        location_filtered_lf = scan_lf.filter.return_value
        location_filtered_lf.filter.assert_called_once()
        is_in_expr = location_filtered_lf.filter.call_args.args[0]
        worker_lf = location_filtered_lf.filter.return_value

        # Polars expressions overload `==` to build a new expression rather than
        # compare equal/unequal, so structural equality is checked via `.meta.eq()`.
        expected_is_in_expr = pl.col(AWKClean.ascwds_worker_import_date).is_in(
            get_matched_ascwds_dates_mock.return_value.implode.return_value
        )
        assert is_in_expr.meta.eq(expected_is_in_expr)

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
