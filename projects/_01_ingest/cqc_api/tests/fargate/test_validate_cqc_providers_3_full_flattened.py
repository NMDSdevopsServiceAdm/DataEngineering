import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.cqc_api.fargate.validate_cqc_providers_3_full_flattened as job

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.validate_cqc_providers_3_full_flattened"
)


class ValidateLocationsFullFlattened(unittest.TestCase):
    def setUp(self) -> None:
        self.validate_df = pl.DataFrame(
            [
                ("1-00001", "20240101", date(2024, 1, 1)),
                ("1-00002", "20240101", date(2024, 1, 1)),
                ("1-00001", "20240201", date(2024, 2, 1)),
                ("1-00002", "20240201", date(2024, 2, 1)),
                ("1-00002", "20240201", date(2024, 2, 1)),
            ],
            schema=pl.Schema(
                [
                    ("providerId", pl.String),
                    ("import_date", pl.String),
                    ("cqc_location_import_date", pl.Date),
                ]
            ),
            orient="row",
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.side_effect = [self.validate_df, self.validate_df]

        job.main("bucket", "my/dataset/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            "s3://bucket/my/dataset/", exclude_complex_types=True
        ),

        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.return_value = self.validate_df

        job.main("bucket", "my/dataset/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        # Extract all assertion types present in the report
        assertion_types_present = {item["assertion_type"] for item in report_json}

        # Check that key validations were run
        expected_assertions = {
            "col_vals_not_null",
            "rows_distinct",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
