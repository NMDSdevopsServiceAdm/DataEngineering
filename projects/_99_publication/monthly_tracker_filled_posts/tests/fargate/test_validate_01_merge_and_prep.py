import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._99_publication.monthly_tracker_filled_posts.fargate.validate_01_merge_and_prep as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate.validate_01_merge_and_prep"

BUCKET_NAME = "bucket"
ESTIMATES_SOURCE = "estimates/"
METADATA_SOURCE = "metadata/"
PREP_SOURCE = "prep/"
REPORTS_PATH = "reports/"
ESTIMATES_RUN = "s3://bucket/estimates/archive_date=2026-10-01/run_number=2/"
METADATA_RUN = "s3://bucket/metadata/archive_date=2026-10-01/run_number=2/"
PREP_RUN = "s3://bucket/prep/archive_date=2026-10-01/run_number=2/"


def _prep_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            IndCQC.id_per_locationid_import_date: [1],
            IndCQC.location_id: ["1-001"],
            IndCQC.cqc_location_import_date: [date(2026, 4, 1)],
            IndCQC.primary_service_type: ["a"],
            IndCQC.main_job_role_clean_labelled: ["a"],
            IndCQC.main_job_group_labelled: ["a"],
            IndCQC.estimate_filled_posts_by_job_role: [1.0],
            IndCQC.current_cssr: ["a"],
            IndCQC.current_region: ["a"],
            IndCQC.current_icb: ["a"],
            IndCQC.current_rural_urban_indicator_2011: ["a"],
            IndCQC.current_lsoa21: ["a"],
            IndCQC.current_msoa21: ["a"],
            Pub.period_label: ["Mar 2026"],
        }
    )


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_runs_validation_on_the_prepared_run(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        scan_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [ESTIMATES_RUN, METADATA_RUN]
        read_parquet_mock.return_value = _prep_df()
        estimates_lf = pl.LazyFrame(
            {IndCQC.cqc_location_import_date: [date(2026, 4, 1)]}
        )
        scan_parquet_mock.return_value = estimates_lf

        job.main(
            BUCKET_NAME, ESTIMATES_SOURCE, METADATA_SOURCE, PREP_SOURCE, REPORTS_PATH
        )

        resolve_run_sources_mock.assert_called_once_with(
            [f"s3://{BUCKET_NAME}/estimates/", f"s3://{BUCKET_NAME}/metadata/"], None
        )
        read_parquet_mock.assert_called_once_with(
            PREP_RUN, selected_columns=[name for name, _ in job.EXPECTED_SCHEMA.columns]
        )
        scan_parquet_mock.assert_called_once_with(
            ESTIMATES_RUN, selected_columns=[IndCQC.cqc_location_import_date]
        )

        validation_arg, bucket_name_arg, reports_path_arg = (
            write_reports_mock.call_args[0]
        )
        assert bucket_name_arg == BUCKET_NAME
        assert reports_path_arg == REPORTS_PATH

        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}
        assert assertion_types_present == {
            "col_schema_match",
            "row_count_match",
            "rows_distinct",
            "col_vals_not_null",
            "col_vals_ge",
            "col_vals_regex",
            "col_vals_expr",
            "col_vals_in_set",
            "specially",
        }

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_expects_row_count_of_estimates_within_retention_window(
        self,
        resolve_run_sources_mock: Mock,
        read_parquet_mock: Mock,
        scan_parquet_mock: Mock,
        write_reports_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [ESTIMATES_RUN, METADATA_RUN]
        read_parquet_mock.return_value = _prep_df()
        estimates_lf = pl.LazyFrame(
            {
                IndCQC.cqc_location_import_date: [
                    date(2026, 4, 1),
                    date(2026, 4, 1),
                    date(2000, 4, 1),
                ]
            }
        )
        scan_parquet_mock.return_value = estimates_lf

        job.main(
            BUCKET_NAME, ESTIMATES_SOURCE, METADATA_SOURCE, PREP_SOURCE, REPORTS_PATH
        )

        validation_arg = write_reports_mock.call_args[0][0]
        row_count_step = next(
            item
            for item in json.loads(validation_arg.get_json_report())
            if item["assertion_type"] == "row_count_match"
        )
        assert row_count_step["values"]["count"] == 2


class TestExpectedSchema:
    def test_schema_covers_columns_selected_by_merge_and_prep(self):
        from projects._99_publication.monthly_tracker_filled_posts.fargate import (
            _01_merge_and_prep as prep_job,
        )

        expected_columns = {
            *prep_job.JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
            *prep_job.JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
            Pub.period_label,
        }

        schema_columns = {name for name, _ in job.EXPECTED_SCHEMA.columns}
        assert schema_columns == expected_columns
