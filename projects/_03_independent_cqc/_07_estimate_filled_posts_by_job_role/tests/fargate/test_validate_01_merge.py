import json
import unittest
from unittest.mock import Mock, patch
from datetime import date

import polars as pl
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            IndCqcColumns.id_per_locationid_import_date: pl.String,
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
        }
        source_rows = [
            ("1", "1-001", date(2026, 1, 1)),
            ("2", "1-002", date(2026, 1, 1)),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df]
        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once_with(
            source="s3://bucket/my/source/",
            selected_columns=list(self.source_df.columns),
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df]

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {"col_vals_not_null", "rows_distinct"}

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
