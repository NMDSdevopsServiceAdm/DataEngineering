import json
import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_04_estimates as job
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    CategoricalColumnTypes,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns, PartitionKeys
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.validate_04_estimates"


class ValidateJobRoleEstimatesTests(unittest.TestCase):
    def setUp(self) -> None:

        source_schema = {
            IndCqcColumns.id_per_locationid_import_date_job_role: pl.String,
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.estimate_filled_posts: pl.Int64,
            IndCqcColumns.primary_service_type: pl.String,
            IndCqcColumns.id_per_locationid_import_date: pl.String,
            IndCqcColumns.main_job_role_clean_labelled: pl.String,
            IndCqcColumns.ascwds_job_role_counts: pl.Int16,
            IndCqcColumns.job_role_filtering_rule: pl.String,
            IndCqcColumns.ascwds_job_role_ratios: pl.Float32,
            IndCqcColumns.imputed_ascwds_job_role_ratios: pl.Float32,
            IndCqcColumns.imputed_ascwds_job_role_counts: pl.Int64,
            IndCqcColumns.estimate_filled_posts_size_group: pl.String,
            IndCqcColumns.ascwds_job_role_rolling_ratio: pl.Float32,
            IndCqcColumns.ascwds_job_role_ratios_merged: pl.Float32,
            IndCqcColumns.ascwds_job_role_ratios_merged_source: pl.String,
            IndCqcColumns.estimate_filled_posts_by_job_role: pl.Float32,
            IndCqcColumns.estimate_filled_posts_by_job_role_historically_reallocated: pl.Float32,
            IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: pl.Int64,
            IndCqcColumns.estimate_filled_posts_from_all_job_roles: pl.Int64,
            IndCqcColumns.difference_estimate_filled_posts_and_from_all_job_roles: pl.Int64,
            IndCqcColumns.main_job_group_labelled: pl.String,
            PartitionKeys.year: pl.String,
        }
        source_rows = [
            ("1", "1-001", date(2026, 1, 1), 100, "service", "1", MainJobRoleLabels.care_worker,    40, "Rule", 0.1, 0.1, 10, "size", 0.1, 0.1, "source", 10.0, 10.0, 10, 10, 10, JobGroupLabels.direct_care, "2026"),
            ("2", "1-002", date(2026, 1, 1), 100, "service", "1", MainJobRoleLabels.support_worker, 30, "Rule", 0.1, 0.1, 10, "size", 0.1, 0.1, "source", 10.0, 10.0, 10, 10, 10, JobGroupLabels.direct_care, "2026"),
        ]  # fmt: skip
        self.source_lf = pl.DataFrame(source_rows, source_schema, orient="row")

        compare_schema = {
            IndCqcColumns.location_id: pl.String,
            IndCqcColumns.cqc_location_import_date: pl.Date,
        }
        compare_rows = [
            ("1-001", date(2026, 1, 1)),
            ("1-002", date(2026, 1, 1)),
        ]
        self.compare_lf = pl.DataFrame(compare_rows, compare_schema, orient="row")

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
            "col_vals_le",
            "conjointly",
        }

        for assertion in expected_assertions:
            self.assertIn(
                assertion,
                assertion_types_present,
                f"{assertion} not found in validation report",
            )


class TestEstimatesPercentageExpressions:
    expected_lf = pl.LazyFrame(
        schema={
            IndCqcColumns.cqc_location_import_date: pl.Date,
            IndCqcColumns.main_job_role_clean_labelled: pl.String,
            IndCqcColumns.main_job_group_labelled: pl.String,
            IndCqcColumns.estimate_filled_posts_by_job_role_manager_adjusted: pl.Float32,
            "expression": pl.Boolean,
        },
        data=[
            (date(2026, 1, 1), MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 60.0, True),
            (date(2026, 1, 1), MainJobRoleLabels.support_worker, JobGroupLabels.direct_care, 20.0, True),
            (date(2026, 1, 1), MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 5.0, True),
            (date(2026, 1, 1), MainJobRoleLabels.data_analyst, JobGroupLabels.other, 10.0, True),
            (date(2026, 1, 1), MainJobRoleLabels.it_manager, JobGroupLabels.managers, 5.0, True),
            (date(2026, 2, 1), MainJobRoleLabels.care_worker, JobGroupLabels.direct_care, 20.0, False),
            (date(2026, 2, 1), MainJobRoleLabels.support_worker, JobGroupLabels.direct_care, 20.0, False),
            (date(2026, 2, 1), MainJobRoleLabels.registered_nurse, JobGroupLabels.regulated_professions, 20.0, False),
            (date(2026, 2, 1), MainJobRoleLabels.data_analyst, JobGroupLabels.other, 20.0, False),
            (date(2026, 2, 1), MainJobRoleLabels.it_manager, JobGroupLabels.managers, 20.0, False),
        ],
        orient="row",
    ) # fmt: skip
    test_lf = expected_lf.drop("expression")

    def test_required_percentages_dictionary(self):
        assert job.req_pcts[MainJobRoleLabels.care_worker] == (0.59, 0.69)
        assert job.req_pcts[JobGroupLabels.direct_care] == (0.71, 0.81)
        assert job.req_pcts[JobGroupLabels.managers] == (0.03, 0.1)
        assert job.req_pcts[JobGroupLabels.regulated_professions] == (0.02, 0.06)
        assert job.req_pcts[JobGroupLabels.other] == (0.07, 0.21)

    def test_estimates_percentage_expressions_for_job_role_when_true(self):
        expr = job.estimates_percentage_expressions(
            MainJobRoleLabels.care_worker,
            job.req_pcts[MainJobRoleLabels.care_worker],
            "role",
        )
        result = self.test_lf.with_columns(expr.alias("expression"))
        pl_testing.assert_frame_equal(result, self.expected_lf)

    def test_estimates_percentage_expressions_for_job_group_when_true(self):
        expr = job.estimates_percentage_expressions(
            JobGroupLabels.direct_care,
            job.req_pcts[JobGroupLabels.direct_care],
            "group",
        )
        result = self.test_lf.with_columns(expr.alias("expression"))
        pl_testing.assert_frame_equal(result, self.expected_lf)

    def test_estimates_percentage_expressions_invalid_role_or_group(self):
        with pytest.raises(ValueError) as excinfo:
            job.estimates_percentage_expressions(
                MainJobRoleLabels.care_worker,
                job.req_pcts[MainJobRoleLabels.care_worker],
                "invalid",
            )

        assert "role_or_group must be either 'role' or 'group'" in str(excinfo.value)

    def test_estimates_percentage_expressions_invalid_pcts(self):
        with pytest.raises(ValueError) as excinfo:
            job.estimates_percentage_expressions(
                MainJobRoleLabels.care_worker,
                (0.5,),  # Invalid pcts tuple
                "role",
            )

        assert "pcts must be a tuple of two values: (lower_bound, upper_bound)" in str(
            excinfo.value
        )


class TestMakeConvertColToIntegersPreprocessor:
    def test_make_convert_col_to_integers_preprocessor(self):
        preprocessor = job.make_convert_col_to_integers_preprocessor(PartitionKeys.year)
        df = pl.DataFrame({PartitionKeys.year: ["2020", "2021", "2022"]})
        result = preprocessor(df)
        expected = pl.DataFrame({PartitionKeys.year: [2020, 2021, 2022]})
        pl_testing.assert_frame_equal(result, expected)

    def test_convert_col_to_integers_with_non_numeric_values(self):
        df = pl.DataFrame({PartitionKeys.year: ["2020", "invalid", "2022"]})
        with pytest.raises(pl.exceptions.InvalidOperationError):
            job.make_convert_col_to_integers_preprocessor(PartitionKeys.year)(df)


if __name__ == "__main__":
    unittest.main(warnings="ignore")
