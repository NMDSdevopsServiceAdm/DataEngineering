import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare"


class ValidatePreparedSLVDataTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
            AWPJobRoles.job_role_code: pl.String,
            AWPJobRoles.employees: pl.Int16,
            AWPJobRoles.starters: pl.Int16,
            AWPJobRoles.leavers: pl.Int16,
            AWPJobRoles.vacancies: pl.Int16,
        }
        source_rows = [
            ("1-001", date(2026, 1, 1), "1", 5, 1, 0, 2),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")
        self.compare_df = self.source_df.select([AWPClean.establishment_id])

        self.compare_schema = {
            AWPClean.establishment_id: pl.String,
            "jr01emp": pl.Int32,
            "jr01strt": pl.Int32,
            "jr01stop": pl.Int32,
            "jr01vacy": pl.Int32,
        }

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df
        mock_scan_parquet.return_value.collect_schema.return_value = self.compare_schema
        mock_scan_parquet.return_value.select.return_value.collect.return_value.height = (
            self.compare_df.height
        )

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        mock_read_parquet.assert_called_once_with(source="s3://bucket/my/source/")
        mock_scan_parquet.assert_called_once_with(source="s3://bucket/my/compare/")
        mock_scan_parquet.return_value.select.assert_called_once_with(
            job.COMPARE_COLS_TO_IMPORT
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.return_value = self.source_df
        mock_scan_parquet.return_value.collect_schema.return_value = self.compare_schema
        mock_scan_parquet.return_value.select.return_value.collect.return_value.height = (
            self.compare_df.height
        )

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
            "rows_distinct",
            "col_vals_not_null",
            "col_vals_between",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
