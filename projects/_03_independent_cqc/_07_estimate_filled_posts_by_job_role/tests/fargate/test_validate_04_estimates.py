import json
import unittest
from unittest.mock import Mock, patch
from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_04_estimates as job
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)

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
            IndCqcColumns.main_job_group_labelled: pl.String,
        }
        source_rows = [
            ("1", "1-001", date(2026, 1, 1), 100.0, "source", MainJobRoleLabels.care_worker,    40.0, 100.0, JobGroupLabels.direct_care),
            ("2", "1-002", date(2026, 1, 1), 100.0, "source", MainJobRoleLabels.support_worker, 30.0, 100.0, JobGroupLabels.direct_care),
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
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_runs(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_lf, self.compare_lf]
        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        self.assertEqual(mock_read_parquet.call_count, 2)
        mock_read_parquet.assert_any_call(
            source="s3://bucket/my/source/",
            selected_columns=list(self.source_lf.collect_schema().keys()),
        )
        mock_read_parquet.assert_any_call(
            source="s3://bucket/my/compare/",
            selected_columns=list(self.compare_lf.collect_schema().keys()),
        )
        mock_write_reports.assert_called_once()

    @patch(f"{PATCH_PATH}.vl.write_reports")
    @patch(f"{PATCH_PATH}.utils.read_parquet")
    def test_validation_report_includes_expected_validations(
        self,
        mock_read_parquet: Mock,
        mock_write_reports: Mock,
    ):
        mock_read_parquet.side_effect = [self.source_lf, self.compare_lf]

        job.main("bucket", "my/source/", "my/compare/", "my/reports/")

        validation_arg = mock_write_reports.call_args[0][0]
        report_json = json.loads(validation_arg.get_json_report())

        assertion_types_present = {item["assertion_type"] for item in report_json}

        expected_assertions = {
            "col_vals_expr",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


class TestEstimatesPercentageExpressions:
    test_lf = pl.LazyFrame(
        schema={
            IndCqcColumns.main_job_role_clean_labelled: pl.String,
            IndCqcColumns.main_job_group_labelled: pl.String,
            IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
        },
        data=[
            (MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 60.0),
            (MainJobRoleLabels.support_worker, JobGroupLabels.direct_care, 20.0),
            (
                MainJobRoleLabels.registered_nurse,
                JobGroupLabels.regulated_professions,
                5.0,
            ),
            (MainJobRoleLabels.data_analyst, JobGroupLabels.other, 10.0),
            (MainJobRoleLabels.it_manager, JobGroupLabels.managers, 5.0),
        ],
        orient="row",
    )
    expected_schema = {
        IndCqcColumns.main_job_role_clean_labelled: pl.String,
        IndCqcColumns.main_job_group_labelled: pl.String,
        IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
        "expression": pl.Boolean,
    }
    expected_true_lf = pl.LazyFrame(
        schema=expected_schema,
        data=[
            (MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 60.0, True),
            (MainJobRoleLabels.support_worker, JobGroupLabels.direct_care, 20.0, True),
            (
                MainJobRoleLabels.registered_nurse,
                JobGroupLabels.regulated_professions,
                5.0,
                True,
            ),
            (MainJobRoleLabels.data_analyst, JobGroupLabels.other, 10.0, True),
            (MainJobRoleLabels.it_manager, JobGroupLabels.managers, 5.0, True),
        ],
        orient="row",
    )
    expected_false_lf = pl.LazyFrame(
        schema=expected_schema,
        data=[
            (MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 60.0, False),
            (MainJobRoleLabels.support_worker, JobGroupLabels.direct_care, 20.0, False),
            (
                MainJobRoleLabels.registered_nurse,
                JobGroupLabels.regulated_professions,
                5.0,
                False,
            ),
            (MainJobRoleLabels.data_analyst, JobGroupLabels.other, 10.0, False),
            (MainJobRoleLabels.it_manager, JobGroupLabels.managers, 5.0, False),
        ],
        orient="row",
    )

    def test_estimates_percentage_expressions_for_job_role_when_true(self):
        expr = job.estimates_percentage_expressions(
            MainJobRoleLabels.care_worker, [0.59, 0.69], "role"
        )
        result = self.test_lf.with_columns(expr.alias("expression"))
        pl_testing.assert_frame_equal(result, self.expected_true_lf)

    def test_estimates_percentage_expressions_for_job_role_when_false(self):
        expr = job.estimates_percentage_expressions(
            MainJobRoleLabels.care_worker, [0.29, 0.39], "role"
        )
        result = self.test_lf.with_columns(expr.alias("expression"))
        pl_testing.assert_frame_equal(result, self.expected_false_lf)

    def test_estimates_percentage_expressions_for_job_group_when_true(self):
        expr = job.estimates_percentage_expressions(
            JobGroupLabels.direct_care, [0.7, 0.8], "group"
        )
        result = self.test_lf.with_columns(expr.alias("expression"))
        pl_testing.assert_frame_equal(result, self.expected_true_lf)

    def test_estimates_percentage_expressions_for_job_group_when_false(self):
        expr = job.estimates_percentage_expressions(
            JobGroupLabels.direct_care, [0.7, 0.79], "group"
        )
        result = self.test_lf.with_columns(expr.alias("expression"))
        pl_testing.assert_frame_equal(result, self.expected_false_lf)

    def test_estimates_percentage_expressions_invalid_role_or_group(self):
        with pytest.raises(ValueError) as excinfo:
            job.estimates_percentage_expressions("care_worker", [0.59, 0.69], "invalid")

        assert "role_or_group must be either 'role' or 'group'" in str(excinfo.value)

    def test_estimates_percentage_expressions_invalid_pcts(self):
        with pytest.raises(ValueError) as excinfo:
            job.estimates_percentage_expressions("care_worker", [0.59], "role")

        assert "pcts must be a list of two values: [lower_bound, upper_bound]" in str(
            excinfo.value
        )


if __name__ == "__main__":
    unittest.main(warnings="ignore")
