import json
import unittest
from unittest.mock import Mock, patch

import polars as pl

import projects._03_independent_cqc._02_clean.fargate.validate_cleaned_ind_cqc_data as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateCleanIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateCleanIndCQCSchemas as Schemas,
)

PATCH_PATH = (
    "projects._03_independent_cqc._02_clean.fargate.validate_cleaned_ind_cqc_data"
)


class ValidateCleanedIndCqcDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            Data.cleaned_ind_cqc_data_rows,
            Schemas.clean_ind_cqc_schema,
            strict=False,
            orient="row",
        )
        self.compare_lf = pl.LazyFrame(
            Data.merged_ind_cqc_data_rows,
            Schemas.merged_ind_cqc_schema,
            strict=False,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.get_expected_row_count")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_get_expected_row_count: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df]
        mock_scan_parquet.side_effect = [self.compare_lf]
        mock_get_expected_row_count.return_value = 4
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_called_once_with(
            "s3://bucket/my/dataset/", exclude_complex_types=True
        )
        mock_scan_parquet.assert_called_once_with(
            "s3://bucket/other/dataset/",
            selected_columns=job.merged_locations_columns_to_import,
        )
        mock_get_expected_row_count.assert_called_once()
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.get_expected_row_count")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_scan_parquet: Mock,
        mock_get_expected_row_count: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df]
        mock_scan_parquet.side_effect = [self.compare_lf]
        mock_get_expected_row_count.return_value = 4
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

    def test_get_expected_row_count_returns_correct_row_count(self):
        test_lf = pl.LazyFrame(
            Data.expected_size_rows,
            Schemas.expected_size_schema,
            orient="row",
        )
        returned_row_count = job.get_expected_row_count(test_lf)
        expected_row_count = 1
        self.assertEqual(returned_row_count, expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
