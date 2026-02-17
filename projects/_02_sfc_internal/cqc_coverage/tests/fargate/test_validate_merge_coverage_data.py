from datetime import date
import json
import unittest
from unittest.mock import Mock, call, patch

import polars as pl

import projects._02_sfc_internal.cqc_coverage.fargate.validate_merge_coverage_data as job
from projects._02_sfc_internal.unittest_data.merged_coverage_data_polars import (
    ValidateMergeCoverageData as Data,
)
from projects._02_sfc_internal.unittest_data.merged_coverage_schema_polars import (
    ValidateMergeCoverageSchemas as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = (
    "projects._02_sfc_internal.cqc_coverage.fargate.validate_merge_coverage_data"
)


class ValidateMergeCoverageDataTests(unittest.TestCase):
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
    @patch(f"{PATCH_PATH}.calculate_expected_size_of_merged_coverage_dataset")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_calculate_expected_size_of_merged_coverage_dataset: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        mock_calculate_expected_size_of_merged_coverage_dataset.return_value = 4
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call("s3://bucket/other/dataset/"),
            ]
        )
        mock_calculate_expected_size_of_merged_coverage_dataset.assert_called_once()
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.calculate_expected_size_of_merged_coverage_dataset")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_calculate_expected_size_of_merged_coverage_dataset: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        mock_calculate_expected_size_of_merged_coverage_dataset.return_value = 4

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        # Extract all assertion types present in the report
        assertion_types_present = {item["assertion_type"] for item in report_json}

        # Check that key validations were run
        expected_assertions = {
            "row_count_match",
            "col_vals_not_null",
            "col_exists",
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

    def test_calculate_expected_size_of_merged_coverage_dataset(self):
        schema = pl.Schema(
            [
                (IndCqcColumns.location_id, pl.String()),
                (IndCqcColumns.cqc_location_import_date, pl.Date()),
                (IndCqcColumns.name, pl.String()),
                (IndCqcColumns.postcode, pl.String()),
                (IndCqcColumns.care_home, pl.String()),
                (Keys.year, pl.Int32()),
                (Keys.month, pl.Int32()),
                (Keys.day, pl.Int32()),
            ]
        )

        input_df = pl.DataFrame(
            data=Data.calculate_expected_size_rows,
            schema=schema,
        )

        result = job.calculate_expected_size_of_merged_coverage_dataset(input_df)

        self.assertEqual(result, Data.expected_row_count)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
