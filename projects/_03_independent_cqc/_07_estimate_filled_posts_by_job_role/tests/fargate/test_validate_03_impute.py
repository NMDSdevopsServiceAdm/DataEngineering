import json
import unittest
from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_03_impute as job
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_03_impute"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:
        source_schema = {
            IndCqcColumns.id_per_locationid_import_date: pl.UInt32,
            IndCqcColumns.id_per_locationid_import_date_job_role: pl.UInt32,
            IndCqcColumns.location_id: CategoricalColumnTypes.LocationCatType,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.main_job_role_clean_labelled: CategoricalColumnTypes.JobRoleEnumType,
            IndCqcColumns.primary_service_type: CategoricalColumnTypes.PrimaryServiceEnumType,
            IndCqcColumns.estimate_filled_posts: pl.Float32,
            IndCqcColumns.ascwds_job_role_counts: pl.Int16,
            IndCqcColumns.job_role_filtering_rule: CategoricalColumnTypes.JobRoleFilteringRuleCatType,
            IndCqcColumns.ascwds_job_role_ratios: pl.Float32,
            IndCqcColumns.imputed_ascwds_job_role_ratios: pl.Float32,
            IndCqcColumns.imputed_ascwds_job_role_counts: pl.Float32,
            IndCqcColumns.estimate_filled_posts_size_group: pl.String,
            IndCqcColumns.ascwds_job_role_rolling_ratio: pl.Float32,
        }
        source_rows = [
            (1, 1, "1-001", date(2026, 1, 1), "care_worker", "non-residential", 10.0, 10, "populated", 0.5, 0.5, 0.5, "NR 1 to 24", 0.5),
            (2, 1, "1-002", date(2026, 1, 1), "care_worker", "non-residential", 10.0, 10, "populated", 0.5, 0.5, 0.5, "NR 1 to 24", 0.5),
        ]  # fmt: skip
        self.source_df = pl.DataFrame(source_rows, source_schema, orient="row")
        self.compare_df = self.source_df.select([IndCqcColumns.location_id])

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
            "col_vals_between",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )

    def test_count_nulls(self):
        test_df = pl.DataFrame(
            {
                IndCqcColumns.imputed_ascwds_job_role_counts: [1, 2, 3, 4],
                IndCqcColumns.ascwds_job_role_counts: [None, None, 3, 4],
                IndCqcColumns.imputed_ascwds_job_role_ratios: [None, None, None, None],
                IndCqcColumns.ascwds_job_role_ratios: [None, None, None, None],
            }
        )
        returned_df = job.count_nulls(test_df)
        expected_df = pl.DataFrame(
            {
                IndCqcColumns.imputed_ascwds_job_role_counts: [0],
                IndCqcColumns.ascwds_job_role_counts: [2],
                IndCqcColumns.imputed_ascwds_job_role_ratios: [4],
                IndCqcColumns.ascwds_job_role_ratios: [4],
            }
        )
        self.assertEqual(
            returned_df[IndCqcColumns.imputed_ascwds_job_role_counts].to_list(),
            expected_df[IndCqcColumns.imputed_ascwds_job_role_counts].to_list(),
        )
        self.assertEqual(
            returned_df[IndCqcColumns.ascwds_job_role_counts].to_list(),
            expected_df[IndCqcColumns.ascwds_job_role_counts].to_list(),
        )
        self.assertEqual(
            returned_df[IndCqcColumns.imputed_ascwds_job_role_ratios].to_list(),
            expected_df[IndCqcColumns.imputed_ascwds_job_role_ratios].to_list(),
        )
        self.assertEqual(
            returned_df[IndCqcColumns.ascwds_job_role_ratios].to_list(),
            expected_df[IndCqcColumns.ascwds_job_role_ratios].to_list(),
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
