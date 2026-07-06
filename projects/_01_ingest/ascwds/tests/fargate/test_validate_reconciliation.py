import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.ascwds.fargate.validate_reconciliation as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._01_ingest.ascwds.fargate.validate_reconciliation"


class ValidateCleanASCWDSWorkplaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            data={
                AWPClean.ascwds_workplace_import_date: date(2026, 1, 1),
                AWPClean.establishment_id: " ",
                AWPClean.nmds_id: " ",
                AWPClean.master_update_date: date(2026, 1, 1),
                AWPClean.master_update_date_org: date(2026, 1, 1),
                AWPClean.establishment_created_date: date(2026, 1, 1),
                AWPClean.is_parent: " ",
                AWPClean.parent_id: " ",
                AWPClean.organisation_id: " ",
                AWPClean.parent_permission: " ",
                AWPClean.establishment_type: " ",
                AWPClean.registration_type: " ",
                AWPClean.location_id: " ",
                AWPClean.main_service_id: " ",
                AWPClean.establishment_name: " ",
                AWPClean.region_id: " ",
                AWPClean.total_staff: 1,
                AWPClean.worker_records: 1,
                AWPClean.last_logged_in_date: date(2026, 1, 1),
                AWPClean.la_permission: " ",
            },
            schema={
                AWPClean.ascwds_workplace_import_date: pl.Date,
                AWPClean.establishment_id: pl.String,
                AWPClean.nmds_id: pl.String,
                AWPClean.master_update_date: pl.Date,
                AWPClean.master_update_date_org: pl.Date,
                AWPClean.establishment_created_date: pl.Date,
                AWPClean.is_parent: pl.String,
                AWPClean.parent_id: pl.String,
                AWPClean.organisation_id: pl.String,
                AWPClean.parent_permission: pl.String,
                AWPClean.establishment_type: pl.String,
                AWPClean.registration_type: pl.String,
                AWPClean.location_id: pl.String,
                AWPClean.main_service_id: pl.String,
                AWPClean.establishment_name: pl.String,
                AWPClean.region_id: pl.String,
                AWPClean.total_staff: pl.Int32,
                AWPClean.worker_records: pl.Int32,
                AWPClean.last_logged_in_date: pl.Date,
                AWPClean.la_permission: pl.String,
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
