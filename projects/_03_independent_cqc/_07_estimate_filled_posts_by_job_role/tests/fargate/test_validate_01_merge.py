import json
import unittest
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge as job
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    MainJobRoleLabels,
    PrimaryServiceType,
)

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_01_merge"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            IndCqcColumns.id_per_locationid_import_date: pl.UInt32,
            IndCqcColumns.location_id: CategoricalColumnTypes.LocationCatType,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.primary_service_type: CategoricalColumnTypes.PrimaryServiceEnumType,
            IndCqcColumns.estimate_filled_posts: pl.Float32,
            IndCqcColumns.estimate_filled_posts_source: CategoricalColumnTypes.EstimatesFilledPostSourceEnumType,
            IndCqcColumns.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleEnumType,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: pl.Float32,
            IndCqcColumns.ascwds_job_role_counts: pl.Int16,
        }
        source_rows = [
            (1, "1-001", date(2026, 1, 1), PrimaryServiceType.non_residential, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.care_worker, 5.0, 10),
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
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        self.assertEqual(mock_read_parquet.call_count, 2)
        mock_read_parquet.assert_has_calls(
            [
                call(
                    source="s3://bucket/my/source/",
                    selected_columns=job.VALIDATION_COLS_TO_IMPORT,
                ),
                call(
                    source="s3://bucket/my/compare/",
                    selected_columns=job.IND_CQC_ESTIMATES_COLS_TO_IMPORT,
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

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_schema_match",
            "row_count_match",
            "col_vals_not_null",
            "rows_distinct",
            "col_vals_expr",
            "col_vals_in_set",
            "specially",
            "col_vals_gt",
            "col_vals_ge",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
