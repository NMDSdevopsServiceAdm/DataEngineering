import json
import unittest
from unittest.mock import Mock, call, patch

import polars as pl

import projects._02_sfc_internal.cqc_coverage.fargate.validate_monthly_coverage_data as job
from projects._02_sfc_internal.unittest_data.merged_coverage_data_polars import (
    ValidateMonthlyCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.merged_coverage_schema_polars import (
    ValidateMonthlyCoverageData as Schemas,
)

PATCH_PATH = (
    "projects._02_sfc_internal.cqc_coverage.fargate.validate_monthly_coverage_data"
)


class ValidateMonthlyCoverageDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            Data.merged_coverage_rows,
            Schemas.merged_coverage_schema,
            strict=False,
            orient="row",
        )
        self.compare_df = pl.DataFrame(
            Data.cqc_locations_rows,
            Schemas.cqc_locations_schema,
            strict=False,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(self, mock_read_parquet: Mock, mock_write_reports: Mock):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call("s3://bucket/other/dataset/"),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self, mock_read_parquet: Mock, mock_write_reports: Mock
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]

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
