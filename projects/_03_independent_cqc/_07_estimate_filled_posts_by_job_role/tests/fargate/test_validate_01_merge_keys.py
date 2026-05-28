import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge_keys as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            IndCqcColumns.id_per_locationid_import_date: pl.String,
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.primary_service_type: pl.String,
            IndCqcColumns.estimate_filled_posts: pl.Float64,
            IndCqcColumns.estimate_filled_posts_source: pl.String,
            IndCqcColumns.main_job_role_clean_labelled: pl.String,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: pl.Float64,
            IndCqcColumns.ascwds_job_role_counts: pl.Float64,
        }
        source_rows = [
            ("1", "1-001", date(2026, 1, 1), "Service A", 10.0, "Source A", "Role A", 5.0, 10.0),
            ("2", "1-002", date(2026, 1, 1), "Service B", 20.0, "Source B", "Role B", 15.0, 20.0),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")
        self.compare_df = self.source_df.select(
            [IndCqcColumns.location_id, IndCqcColumns.cqc_location_import_date]
        )

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_df, self.compare_df]
        job.main("bucket", "my/source/", "my/reports/")

        mock_read_parquet.assert_called_once()
        mock_read_parquet.assert_any_call(
            source="s3://bucket/my/source/",
            selected_columns=job.KEY_COLS,
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

        job.main("bucket", "my/source/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "rows_distinct",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
