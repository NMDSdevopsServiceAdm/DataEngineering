import json
import unittest
from unittest.mock import Mock, patch
from datetime import date

import polars as pl
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_04_estimates as job

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_04_estimates"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            IndCqcColumns.id_per_locationid_import_date: pl.String,
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.estimate_filled_posts: pl.Float32,
            IndCqcColumns.ascwds_job_role_ratios_merged_source: pl.String,
            IndCqcColumns.main_job_role_clean_labelled: pl.String,
            IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
            IndCqcColumns.estimate_filled_posts_from_all_job_roles: pl.Float32,
        }
        source_rows = [
            ("1", "1-001", date(2026, 1, 1), 100.0, "source", "care_worker",    40.0, 100.0),
            ("2", "1-002", date(2026, 1, 1), 100.0, "source", "support_worker", 30.0, 100.0),
        ]  # fmt: skip
        self.source_lf = pl.LazyFrame(source_rows, source_schema, orient="row")

        compare_schema = {
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
        }
        compare_rows = [
            ("1-001", date(2026, 1, 1)),
            ("1-002", date(2026, 1, 1)),
        ]
        self.compare_lf = pl.LazyFrame(compare_rows, compare_schema, orient="row")

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_validation_runs(
        self,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_scan_parquet.side_effect = [self.source_lf, self.compare_lf]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        self.assertEqual(mock_scan_parquet.call_count, 2)
        mock_scan_parquet.assert_any_call(
            source="s3://bucket/my/source/",
            selected_columns=list(self.source_lf.collect_schema().keys()),
        )
        mock_scan_parquet.assert_any_call(
            source="s3://bucket/my/compare/",
            selected_columns=list(self.compare_lf.collect_schema().keys()),
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_scan_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_scan_parquet.side_effect = [self.source_lf, self.compare_lf]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_vals_eq",
            "col_vals_not_null",
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
