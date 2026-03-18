import json
import unittest
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.validate_estimated_ind_cqc_filled_posts_data as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateEstimatedIndCQCFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateEstimatedIndCQCFilledPostsSchemas as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.validate_estimated_ind_cqc_filled_posts_data"


class ValidateEstimatedIndCQCFilledPostsDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source_df = pl.DataFrame(
            Data.estimated_ind_cqc_filled_posts_rows,
            Schemas.estimated_ind_cqc_filled_posts_schema,
            strict=False,
            orient="row",
        )
        self.compare_df = pl.DataFrame(
            Data.imputed_ind_cqc_rows,
            Schemas.imputed_ind_cqc_schema,
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
                    selected_columns=job.imputed_ind_cqc_cols_to_import,
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
