import json
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ascwds.fargate.validate_clean_ascwds_worker_data as job
from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as ASCWKClean,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.validate_clean_ascwds_worker_data"


def build_source_df() -> pl.DataFrame:
    return pl.DataFrame(
        data={
            ASCWKClean.location_id: "1-000000001",
            ASCWKClean.establishment_id: "100",
            ASCWKClean.worker_id: "1",
            ASCWKClean.main_job_role_id: "8",
            ASCWKClean.ascwds_worker_import_date: date(2000, 1, 1),
            ASCWKClean.main_job_role_clean: "8",
            ASCWKClean.main_job_role_clean_labelled: "care_worker",
        },
        schema={
            ASCWKClean.location_id: pl.String,
            ASCWKClean.establishment_id: pl.String,
            ASCWKClean.worker_id: pl.String,
            ASCWKClean.main_job_role_id: pl.String,
            ASCWKClean.ascwds_worker_import_date: pl.Date,
            ASCWKClean.main_job_role_clean: CategoricalColumnTypes.MainJobRoleIdCatType,
            ASCWKClean.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleCatType,
        },
    )


class TestMain:
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_reads_from_expected_source_path(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = build_source_df()

        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_writes_reports_once(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = build_source_df()

        job.main("bucket", "my/source/", "my/reports/")

        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_report_includes_all_expected_check_types(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = build_source_df()

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())
        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_schema_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_in_set",
            "specially",
        }
        assert expected_assertions <= assertion_types_present
