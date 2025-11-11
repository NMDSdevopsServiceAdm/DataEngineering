import json
import unittest
from datetime import date
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._01_ingest.cqc_api.fargate.validate_cqc_locations_4_full_latest_snapshot as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    ValidateCqcLocations4FullLatestSnapshotTest as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    ValidateCqcLocations4FullLatestSnapshotTest as Schemas,
)

PATCH_PATH = (
    "projects._01_ingest.cqc_api.fargate.validate_cqc_locations_4_full_latest_snapshot"
)


class ValidateLocationsFlattenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validate_df = pl.DataFrame(
            data=Data.validation_rows, schema=Schemas.validation_schema
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.side_effect = [self.validate_df, self.validate_df]

        job.main("bucket", "my/dataset/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            "s3://bucket/my/dataset/",
            exclude_complex_types=True,
        )
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
            "col_vals_in_set",
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
