import json
import unittest
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.validate_00_prepare"


class ValidatePreparedSLVDataTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            AWPClean.establishment_id: pl.String,
        }
        source_rows = [
            ("1-001"),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

        # The compare frame is the unreduced cleaned ASCWDS data, so it carries a row
        # the reduction filter drops. Dates are chosen to stay stable as time passes:
        # January always survives (quarterly sampling) and a pre-window May never does.
        compare_schema = {
            AWPClean.establishment_id: pl.String,
            AWPClean.ascwds_workplace_import_date: pl.Date,
        }
        compare_rows = [
            ("1-001", date(2026, 1, 1)),
            ("1-002", date(2020, 5, 1)),
        ]  # fmt: skip
        self.compare_df = pl.DataFrame(compare_rows, compare_schema, orient="row")

    # TEMPORARY - ticket 1820. Drop the dHelpers patch when the instrumentation goes.
    @patch(f"{PATCH_PATH}.dHelpers.write_checkpoint")
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
        mock_write_checkpoint: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        self.assertEqual(mock_read_parquet.call_count, 2)
        mock_read_parquet.assert_has_calls(
            [
                call(source="s3://bucket/my/source/"),
                call(
                    source="s3://bucket/my/compare/",
                    selected_columns=job.COMPARE_COLS_TO_IMPORT,
                ),
            ]
        )
        mock_write_reports.assert_called_once()

    # TEMPORARY - ticket 1820. Drop the dHelpers patch when the instrumentation goes.
    @patch(f"{PATCH_PATH}.dHelpers.write_checkpoint")
    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
        mock_write_checkpoint: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "row_count_match",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
