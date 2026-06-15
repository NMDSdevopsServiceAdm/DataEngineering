import unittest

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.clean_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRoleCleanUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateFilledPostsByJobRoleCleanUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
)


class NullifyJobRoleCountWhenSourceNotAscwds(unittest.TestCase):
    def setUp(self) -> None:
        self.test_schema = {
            IndCQC.ascwds_filled_posts_dedup_clean: pl.Float64,
            IndCQC.estimate_filled_posts: pl.Float64,
            IndCQC.estimate_filled_posts_source: pl.String,
            IndCQC.ascwds_job_role_counts: pl.Int64,
        }
        self.input_rows_that_meet_condition = [
            (10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, 1),
            (10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, 2),
        ]

        self.expected_rows_that_meet_condition = self.input_rows_that_meet_condition

    def _create_input_lf(self, extra_rows: list[tuple]) -> pl.LazyFrame:
        """Set the input LazyFrame up with rows that meet condition + given rows."""
        return pl.LazyFrame(
            [*self.input_rows_that_meet_condition, *extra_rows],
            self.test_schema,
            orient="row",
        )

    def _create_expected_lf(self, extra_rows: list[tuple]) -> pl.LazyFrame:
        """Set the expected LazyFrame up with rows that meet condition + given rows."""
        return pl.LazyFrame(
            [*self.expected_rows_that_meet_condition, *extra_rows],
            self.test_schema,
            orient="row",
        )

    def test_nullifies_when_source_not_ascwds(self):
        input_lf = self._create_input_lf(
            # Row with non-ASCWDS source
            [(10.0, 10.0, EstimateFilledPostsSource.care_home_model, 2)],
        )
        expected_lf = self._create_expected_lf(
            # Row with non-ASCWDS source (counts nullified)
            [(10.0, 10.0, EstimateFilledPostsSource.care_home_model, None)],
        )
        returned_lf = job.nullify_job_role_count_when_source_not_ascwds(input_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_nullifies_when_estimate_doesnt_match_ascwds(self):
        input_lf = self._create_input_lf(
            # Row that doesn't match estimate filled posts.
            [(None, 20.0, EstimateFilledPostsSource.ascwds_pir_merged, 1)],
        )
        expected_lf = self._create_expected_lf(
            # Row that doesn't match estimate filled posts (counts nullified)
            [(None, 20.0, EstimateFilledPostsSource.ascwds_pir_merged, None)],
        )
        returned_lf = job.nullify_job_role_count_when_source_not_ascwds(input_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestFilterAscwdsJobRoleCountWhenJobGroupRatiosOutsidePercentileBounds:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_when_job_group_ratio_outside_percentile_bounds_test_cases
        ],
    )
    def test_filter_when_job_group_ratio_outside_percentile_bounds(self, case):
        test_lf = pl.LazyFrame(case.test_data, Schemas.test_filter_schema, orient="row")
        expected_lf = pl.LazyFrame(
            case.expected_data, Schemas.expected_filter_schema, orient="row"
        )

        returned_lf = job.filter_job_role_group_outliers(
            test_lf, case.small_location_threshold
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )


class TestFilterJobRoleGroupExpressions:
    TestExprs = job.FilterJobRoleGroupExpressions()

    def test_variables_in_filter_job_role_group_expressions(self):
        assert self.TestExprs.temp_location_sum == "location_sum"
        assert self.TestExprs.job_group_cols == [
            JobGroupLabels.direct_care,
            JobGroupLabels.managers,
            JobGroupLabels.regulated_professions,
            JobGroupLabels.other,
        ]
        assert self.TestExprs.upper_bound_suffix == "_upper"
        assert self.TestExprs.lower_bound_suffix == "_lower"

    def test_location_sum_expression(self):
        expected_lf = pl.LazyFrame(
            Data.test_location_sum_expr_rows,
            Schemas.test_location_sum_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(self.TestExprs.temp_location_sum)
        returned_lf = test_lf.with_columns(self.TestExprs.location_sum_expr)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_job_group_percentage_expression(self):
        test_lf = pl.LazyFrame(
            Data.test_location_sum_expr_rows,
            Schemas.test_location_sum_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.test_job_group_percentage_rows,
            Schemas.test_job_group_percentage_schema,
        )
        returned_lf = test_lf.with_columns(self.TestExprs.job_group_percentage_expr)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_job_group_percentage_handles_null_and_zero(self):
        # Zero denominator should yield NULL percentages
        test_lf_zero = pl.LazyFrame(
            [(None, None, None, None, 0)],
            Schemas.test_location_sum_schema,
            orient="row",
        )
        returned_zero = test_lf_zero.with_columns(
            self.TestExprs.job_group_percentage_expr
        )
        expected_zero = pl.LazyFrame(
            [(None, None, None, None, 0)],
            Schemas.test_job_group_percentage_schema,
        )
        pl_testing.assert_frame_equal(returned_zero, expected_zero)

        # NULL denominator should also yield NULL percentages
        test_lf_null = pl.LazyFrame(
            [(None, None, None, None, None)],
            Schemas.test_location_sum_schema,
            orient="row",
        )
        returned_null = test_lf_null.with_columns(
            self.TestExprs.job_group_percentage_expr
        )
        expected_null = pl.LazyFrame(
            [(None, None, None, None, None)],
            Schemas.test_job_group_percentage_schema,
        )
        pl_testing.assert_frame_equal(returned_null, expected_null)

    def test_evaluation_expression(self):
        test_lf = pl.LazyFrame(
            Data.test_evaluation_expr_rows,
            Schemas.test_evaluation_expr_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.expected_evaluation_expr_rows,
            Schemas.test_evaluation_expr_schema,
            orient="row",
        )
        returned_lf = test_lf.with_columns(
            pl.when(self.TestExprs.evaluation_expr)
            .then(None)
            .otherwise(pl.col(IndCQC.ascwds_job_role_counts))
            .alias(IndCQC.ascwds_job_role_counts)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestFilterJobRoleGroupsEqualZero:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_job_role_group_equal_zero_test_cases
        ],
    )
    def test_function_returns_expected_values(self, case):
        test_lf = pl.LazyFrame(
            case.test_data,
            Schemas.test_job_role_group_equal_zero_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.test_job_role_group_equal_zero_schema,
            orient="row",
        )

        returned_lf = job.filter_job_role_group_equal_zero(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, check_row_order=False
        )
