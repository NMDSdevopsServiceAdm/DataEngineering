import json
import unittest
from datetime import date
from unittest.mock import ANY, Mock, call, patch

import polars as pl

import projects._03_independent_cqc._04_model.fargate.validate_model_01_features_non_res_with_dormancy as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ValidateModel01FeaturesNonResWithDormancyData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ValidateModel01FeaturesNonResWithDormancySchemas as Schemas,
)

PATCH_PATH = "projects._03_independent_cqc._04_model.fargate.validate_model_01_features_non_res_with_dormancy"


class ValidateModelFeaturesNonResWithDormancy(unittest.TestCase):
    def setUp(self) -> None:
        self.validate_df = pl.DataFrame(
            data=Data.validation_rows,
            schema=Schemas.validation_schema,
            strict=False,
            orient="row",
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(
        f"{PATCH_PATH}.get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy"
    )
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_get_expected_row_count: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.validate_df, self.validate_df]
        mock_get_expected_row_count.return_value = (
            Data.expected_get_expected_row_count_rows
        )

        job.main("bucket", "my/dataset/", "my/reports/", "other/dataset/")

        mock_read_parquet.assert_has_calls(
            [
                call("s3://bucket/my/dataset/", exclude_complex_types=False),
                call("s3://bucket/other/dataset/"),
            ]
        )
        mock_get_expected_row_count.assert_called_once()
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(
        f"{PATCH_PATH}.get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy"
    )
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_get_expected_row_count: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.validate_df, self.validate_df]
        mock_get_expected_row_count.return_value = (
            Data.expected_get_expected_row_count_rows
        )

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
