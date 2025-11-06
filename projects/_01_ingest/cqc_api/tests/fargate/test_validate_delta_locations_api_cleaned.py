import json
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.cqc_api.fargate.validate_delta_locations_api_cleaned as job
from schemas.cqc_locations_cleaned_schema_polars import POLARS_CLEANED_LOCATIONS_SCHEMA
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.validate_delta_locations_api_cleaned"
CWD = Path(__file__).parent
RAW_LOCATIONS_DATA = CWD / "test_data" / "delta_locations_api_raw.json"
CLEANED_LOCATIONS_DATA = CWD / "test_data" / "delta_locations_api_cleaned.json"


class ValidateLocationsRawTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            data=json.loads(RAW_LOCATIONS_DATA.read_text()),
            schema=POLARS_LOCATION_SCHEMA,
        )
        self.compare_df = pl.DataFrame(
            data=json.loads(CLEANED_LOCATIONS_DATA.read_text()),
            schema=POLARS_CLEANED_LOCATIONS_SCHEMA,
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.side_effect = [self.compare_df, self.source_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call("s3://bucket/other/dataset/", selected_columns=ANY),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.side_effect = [self.compare_df, self.source_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        # Extract all assertion types present in the report
        assertion_types_present = {item["assertion_type"] for item in report_json}

        # Check that key validations were run
        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_ge",
            "col_vals_between",
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
