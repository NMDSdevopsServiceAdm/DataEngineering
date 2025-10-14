import json
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.cqc_api.fargate.validate_delta_locations_api_cleaned as job
from schemas.cqc_locations_cleaned_schema_polars import POLARS_CLEANED_LOCATIONS_SCHEMA
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA

PATCH_PATH = "polars_utils"
CWD = Path(__file__).parent
RAW_LOCATIONS_DATA = CWD / "test_data" / "delta_locations_api_raw.json"
CLEANED_LOCATIONS_DATA = CWD / "test_data" / "delta_locations_api_cleaned.json"


class ValidateLocationsRawTests(unittest.TestCase):
    def setUp(self) -> None:
        self.raw_df = pl.DataFrame(
            data=json.loads(RAW_LOCATIONS_DATA.read_text()),
            schema=POLARS_LOCATION_SCHEMA,
        )
        self.cleaned_df = pl.DataFrame(
            data=json.loads(CLEANED_LOCATIONS_DATA.read_text()),
            schema=POLARS_CLEANED_LOCATIONS_SCHEMA,
        )

    @patch(f"{PATCH_PATH}.validation.actions.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_invalid_dataset(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        # Given
        # Assumes that validation script reads in cleaned df first, then raw (for comparison)
        mock_read_parquet.side_effect = [self.cleaned_df, self.raw_df]

        # When
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        # Then
        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call("s3://bucket/other/dataset/", selected_columns=ANY),
            ]
        )
        mock_write_reports.assert_called_once()

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        self.assertDictContainsSubset(
            {
                "assertion_type": "row_count_match",
                "all_passed": False,
            },
            report_json[0],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_not_null",
                "column": "locationId",
                "all_passed": True,
            },
            report_json[1],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_not_null",
                "column": "providerId",
                "all_passed": False,
            },
            report_json[3],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "rows_distinct",
                "column": ["locationId", "cqc_location_import_date"],
                "all_passed": True,
            },
            report_json[8],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_ge",
                "column": "numberOfBeds",
                "all_passed": False,
                "na_pass": True,
            },
            report_json[9],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_between",
                "column": "locationId_length",
                "all_passed": False,
                "n_failed": 2,
            },
            report_json[10],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_between",
                "column": "providerId_length",
                "all_passed": False,
                "n_failed": 2,
            },
            report_json[11],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_in_set",
                "column": "cqc_sector",
                "all_passed": False,
                "values": [
                    "Local authority",
                    "Independent",
                ],
            },
            report_json[12],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "col_vals_in_set",
                "column": "dormancy",
                "all_passed": True,
                "values": ["Y", "N", None],
            },
            report_json[14],
        )
        self.assertDictContainsSubset(
            {
                "assertion_type": "specially",
                "all_passed": True,
                "brief": "dormancy needs to be null, or one of ['Y', 'N']",
            },
            report_json[18],
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
