import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ascwds.fargate.validate_clean_ascwds_worker_data as job
from polars_utils.column_types import CategoricalColumnTypes
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.validate_clean_ascwds_worker_data"


class ValidateCleanASCWDSWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            data={
                AWKClean.location_id: "1-001",
                AWKClean.establishment_id: "101",
                AWKClean.worker_id: "1000",
                AWKClean.main_job_role_id: "1",
                AWKClean.ascwds_worker_import_date: date(2026, 1, 1),
                AWKClean.main_job_role_clean: "1",
                AWKClean.main_job_role_clean_labelled: "senior_management",
            },
            schema={
                AWKClean.location_id: pl.String,
                AWKClean.establishment_id: pl.String,
                AWKClean.worker_id: pl.String,
                AWKClean.main_job_role_id: pl.String,
                AWKClean.ascwds_worker_import_date: pl.Date,
                AWKClean.main_job_role_clean: CategoricalColumnTypes.MainJobRoleIdEnumType,
                AWKClean.main_job_role_clean_labelled: CategoricalColumnTypes.MainJobRoleLabelEnumType,
            },
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df
        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df

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

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
