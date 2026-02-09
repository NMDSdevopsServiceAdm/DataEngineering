import json
import unittest
from unittest.mock import Mock, call, patch

import polars as pl
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
import projects._03_independent_cqc._01_merge.fargate.validate_merged_ind_cqc_data as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateMergeIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateMergeIndCQCSchemas as Schemas,
)

PATCH_PATH = (
    "projects._03_independent_cqc._01_merge.fargate.validate_merged_ind_cqc_data"
)

cleaned_cqc_locations_columns_to_import = [
    CQCLClean.cqc_location_import_date,
    CQCLClean.location_id,
    CQCLClean.cqc_sector,
]


class ValidateMergeIndCqcDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            Data.merged_ind_cqc_data_rows,
            Schemas.merged_ind_cqc_schema,
            strict=False,
            orient="row",
        )
        self.compare_df = pl.DataFrame(
            Data.cqc_locations_cleaned_data_rows,
            Schemas.cqc_locations_cleaned_schema,
            strict=False,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=True),
                call(
                    "s3://bucket/other/dataset/",
                    selected_columns=cleaned_cqc_locations_columns_to_import,
                ),
            ]
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
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
