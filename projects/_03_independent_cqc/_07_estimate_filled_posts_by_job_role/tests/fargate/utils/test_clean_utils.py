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
    JobRoleFilteringRule,
    PrimaryServiceType,
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
    def test_filter_when_location_and_job_group_ratio_outside_percentile_bounds(
        self, case
    ):
        test_lf = pl.LazyFrame(
            case.test_data, Schemas.test_filter_location_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_data, Schemas.expected_filter_location_schema, orient="row"
        )

        returned_lf = job.filter_job_role_group_outliers(
            test_lf,
            id_column=IndCQC.location_id,
            min_workers_threshold=case.min_workers_threshold,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_when_job_group_ratio_outside_percentile_bounds_test_cases
        ],
    )
    def test_filter_when_provider_and_job_group_ratio_outside_percentile_bounds(
        self, case
    ):
        test_lf = pl.LazyFrame(
            case.test_data, Schemas.test_filter_provider_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_brand_prov_data,
            Schemas.expected_filter_provider_schema,
            orient="row",
        ).with_columns(
            pl.when(
                pl.col(IndCQC.job_role_filtering_rule)
                == JobRoleFilteringRule.job_role_group_is_outlier_at_location_level
            )
            .then(
                pl.lit(JobRoleFilteringRule.job_role_group_is_outlier_at_provider_level)
            )
            .otherwise(pl.col(IndCQC.job_role_filtering_rule))
            .alias(IndCQC.job_role_filtering_rule)
        )

        returned_lf = job.filter_job_role_group_outliers(
            test_lf,
            id_column=IndCQC.provider_id,
            min_workers_threshold=case.min_workers_threshold,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )

    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_when_job_group_ratio_outside_percentile_bounds_test_cases
        ],
    )
    def test_filter_when_brand_and_job_group_ratio_outside_percentile_bounds(
        self, case
    ):
        test_lf = pl.LazyFrame(
            case.test_data, Schemas.test_filter_brand_schema, orient="row"
        )
        expected_lf = pl.LazyFrame(
            case.expected_brand_prov_data,
            Schemas.expected_filter_brand_schema,
            orient="row",
        ).with_columns(
            pl.when(
                pl.col(IndCQC.job_role_filtering_rule)
                == JobRoleFilteringRule.job_role_group_is_outlier_at_location_level
            )
            .then(pl.lit(JobRoleFilteringRule.job_role_group_is_outlier_at_brand_level))
            .otherwise(pl.col(IndCQC.job_role_filtering_rule))
            .alias(IndCQC.job_role_filtering_rule)
        )

        returned_lf = job.filter_job_role_group_outliers(
            test_lf,
            id_column=IndCQC.brand_id,
            min_workers_threshold=case.min_workers_threshold,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
        )

    def test_error_handling(self):
        test_lf = pl.LazyFrame(
            Data.test_error_handling_data,
            Schemas.test_filter_brand_schema,
            orient="row",
        )
        expected_error = f"Value must be one of {IndCQC.brand_id}, {IndCQC.provider_id}, or {IndCQC.location_id}"
        with pytest.raises(ValueError, match=expected_error):
            job.filter_job_role_group_outliers(test_lf, id_column="other")


class TestFilterJobRoleGroupExpressions:
    TestExprs = job.FilterJobRoleGroupExpressions()

    def test_job_role_group_bounds_dict(self):
        expected_bounds = {
            PrimaryServiceType.care_home_only: {
                f"{JobGroupLabels.direct_care}{self.TestExprs.upper_bound_suffix}": 0.985761,
                f"{JobGroupLabels.managers}{self.TestExprs.upper_bound_suffix}": 0.307057,
                f"{JobGroupLabels.regulated_professions}{self.TestExprs.upper_bound_suffix}": 0.161988,
                f"{JobGroupLabels.other}{self.TestExprs.upper_bound_suffix}": 0.569972,
                f"{JobGroupLabels.direct_care}{self.TestExprs.lower_bound_suffix}": 0.264068,
            },
            PrimaryServiceType.care_home_with_nursing: {
                f"{JobGroupLabels.direct_care}{self.TestExprs.upper_bound_suffix}": 0.943761,
                f"{JobGroupLabels.managers}{self.TestExprs.upper_bound_suffix}": 0.222222,
                f"{JobGroupLabels.regulated_professions}{self.TestExprs.upper_bound_suffix}": 0.350631,
                f"{JobGroupLabels.other}{self.TestExprs.upper_bound_suffix}": 0.964286,
                f"{JobGroupLabels.direct_care}{self.TestExprs.lower_bound_suffix}": 0.012821,
            },
            PrimaryServiceType.non_residential: {
                f"{JobGroupLabels.direct_care}{self.TestExprs.upper_bound_suffix}": 0.995851,
                f"{JobGroupLabels.managers}{self.TestExprs.upper_bound_suffix}": 0.335846,
                f"{JobGroupLabels.regulated_professions}{self.TestExprs.upper_bound_suffix}": 0.338843,
                f"{JobGroupLabels.other}{self.TestExprs.upper_bound_suffix}": 0.576850,
                f"{JobGroupLabels.direct_care}{self.TestExprs.lower_bound_suffix}": 0.233974,
            },
        }
        assert self.TestExprs.job_role_group_bounds == expected_bounds

    def test_variables_in_filter_job_role_group_expressions(self):
        assert self.TestExprs.temp_id_column_sum == "location_sum"
        assert self.TestExprs.job_group_cols == [
            JobGroupLabels.direct_care,
            JobGroupLabels.managers,
            JobGroupLabels.regulated_professions,
            JobGroupLabels.other,
        ]
        assert self.TestExprs.upper_bound_suffix == "_upper"
        assert self.TestExprs.lower_bound_suffix == "_lower"

    def test_id_column_sum_expression(self):
        expected_lf = pl.LazyFrame(
            Data.test_id_column_sum_expr_rows,
            Schemas.test_location_sum_schema,
            orient="row",
        )
        test_lf = expected_lf.drop(self.TestExprs.temp_id_column_sum)
        returned_lf = test_lf.with_columns(self.TestExprs.id_column_sum_expr)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_job_group_percentage_expression(self):
        test_lf = pl.LazyFrame(
            Data.test_id_column_sum_expr_rows,
            Schemas.test_location_sum_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            Data.test_job_group_percentage_rows,
            Schemas.test_job_group_percentage_schema,
            orient="row",
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
            orient="row",
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
            orient="row",
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
