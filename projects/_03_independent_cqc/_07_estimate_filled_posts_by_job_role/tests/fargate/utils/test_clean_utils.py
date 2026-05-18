import unittest
import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.clean_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    JobGroupLabels,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateFilledPostsByJobRoleCleanUtilsData as Data,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
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
        schema = {
            IndCQC.id_per_locationid_import_date: pl.Int64,
            IndCQC.location_id: pl.String,
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.primary_service_type: pl.String,
            IndCQC.main_job_role_clean_labelled: pl.Enum(
                AscwdsWorkerValueLabelsJobGroup.all_roles()
            ),
            IndCQC.ascwds_job_role_counts: pl.Int64,
        }
        test_lf = pl.LazyFrame(case.test_data, schema, orient="row")
        expected_lf = pl.LazyFrame(case.expected_data, schema, orient="row")

        returned_lf = job.filter_job_role_group_outliers(
            test_lf, case.upper_bound, case.lower_bound
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestFilterJobRoleGroupExpressions:
    test_upper_bound = 0.8
    test_lower_bound = 0.2
    TestExprs = job.FilterJobRoleGroupExpressions(test_upper_bound, test_lower_bound)

    def test_variables_in_filter_job_role_group_expressions(self):
        assert self.TestExprs.temp_location_sum == "location_sum"
        assert self.TestExprs.job_group_cols == [
            JobGroupLabels.direct_care,
            JobGroupLabels.managers,
            JobGroupLabels.regulated_professions,
            JobGroupLabels.other,
        ]
        assert self.TestExprs.bounds == [self.test_upper_bound, self.test_lower_bound]
        assert self.TestExprs.suffixes == ["_upper_bound", "_lower_bound"]

    def test_location_sum_expression(self):
        expected_lf = pl.LazyFrame(
            data={
                JobGroupLabels.direct_care: [1, 2],
                JobGroupLabels.managers: [3, 4],
                JobGroupLabels.regulated_professions: [5, 6],
                JobGroupLabels.other: [7, 8],
                self.TestExprs.temp_location_sum: [16, 20],
            }
        )
        test_lf = expected_lf.drop(self.TestExprs.temp_location_sum)
        returned_lf = test_lf.with_columns(self.TestExprs.location_sum_expr)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_job_group_percentage_expression(self):
        test_lf = pl.LazyFrame(
            data={
                JobGroupLabels.direct_care: [1, 2],
                JobGroupLabels.managers: [3, 4],
                JobGroupLabels.regulated_professions: [5, 6],
                JobGroupLabels.other: [7, 8],
                self.TestExprs.temp_location_sum: [16, 20],
            }
        )
        expected_lf = pl.LazyFrame(
            data={
                JobGroupLabels.direct_care: [0.0625, 0.1],
                JobGroupLabels.managers: [0.1875, 0.2],
                JobGroupLabels.regulated_professions: [0.3125, 0.3],
                JobGroupLabels.other: [0.4375, 0.4],
                self.TestExprs.temp_location_sum: [16, 20],
            }
        )
        returned_lf = test_lf.with_columns(self.TestExprs.job_group_percentage_expr)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_evaluation_expression(self):
        schema = {
            JobGroupLabels.direct_care: pl.Float32,
            JobGroupLabels.managers: pl.Float32,
            JobGroupLabels.regulated_professions: pl.Float32,
            JobGroupLabels.other: pl.Float32,
            (
                JobGroupLabels.direct_care + self.TestExprs.upper_bound_suffix
            ): pl.Float32,
            (JobGroupLabels.managers + self.TestExprs.upper_bound_suffix): pl.Float32,
            (
                JobGroupLabels.regulated_professions + self.TestExprs.upper_bound_suffix
            ): pl.Float32,
            (JobGroupLabels.other + self.TestExprs.upper_bound_suffix): pl.Float32,
            (
                JobGroupLabels.direct_care + self.TestExprs.lower_bound_suffix
            ): pl.Float32,
            "location_out_of_bounds": pl.Boolean,
        }
        data = [
            (0.1, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, False), # All within bounds
            (0.3, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, True), # Direct care above upper bound
            (0.1, 0.3, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, True), # Managers above upper bound
            (0.1, 0.1, 0.3, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, True), # Regulated professionals above upper bound
            (0.1, 0.1, 0.1, 0.3, 0.2, 0.2, 0.2, 0.2, 0.05, True), # Other above upper bound
            (0.01, 0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.05, True), # Direct care below lower bound
            (0.01, 0.3, 0.3, 0.3, 0.2, 0.2, 0.2, 0.2, 0.05, True), # All out of bounds
        ] # fmt: skip
        expected_lf = pl.LazyFrame(data=data, schema=schema, orient="row")
        test_lf = expected_lf.drop("location_out_of_bounds")
        returned_lf = test_lf.with_columns(
            pl.when(self.TestExprs.evaluation_expr)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .cast(pl.Boolean)
            .alias("location_out_of_bounds")
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_bounds_expressions(self):
        schema = {
            JobGroupLabels.managers: pl.Float32,
            JobGroupLabels.direct_care: pl.Float32,
            JobGroupLabels.regulated_professions: pl.Float32,
            JobGroupLabels.other: pl.Float32,
            JobGroupLabels.direct_care + self.TestExprs.upper_bound_suffix: pl.Float32,
            JobGroupLabels.managers + self.TestExprs.upper_bound_suffix: pl.Float32,
            JobGroupLabels.regulated_professions
            + self.TestExprs.upper_bound_suffix: pl.Float32,
            JobGroupLabels.other + self.TestExprs.upper_bound_suffix: pl.Float32,
            JobGroupLabels.direct_care + self.TestExprs.lower_bound_suffix: pl.Float32,
            JobGroupLabels.managers + self.TestExprs.lower_bound_suffix: pl.Float32,
            JobGroupLabels.regulated_professions
            + self.TestExprs.lower_bound_suffix: pl.Float32,
            JobGroupLabels.other + self.TestExprs.lower_bound_suffix: pl.Float32,
        }
        expected_lf = pl.LazyFrame(
            data={
                JobGroupLabels.direct_care: [0.0625, 0.1],
                JobGroupLabels.managers: [0.1875, 0.2],
                JobGroupLabels.regulated_professions: [0.3125, 0.3],
                JobGroupLabels.other: [0.4375, 0.4],
                JobGroupLabels.direct_care
                + self.TestExprs.upper_bound_suffix: [0.0925] * 2,
                JobGroupLabels.managers
                + self.TestExprs.upper_bound_suffix: [0.1975] * 2,
                JobGroupLabels.regulated_professions
                + self.TestExprs.upper_bound_suffix: [0.31] * 2,
                JobGroupLabels.other + self.TestExprs.upper_bound_suffix: [0.43] * 2,
                JobGroupLabels.direct_care
                + self.TestExprs.lower_bound_suffix: [0.07] * 2,
                JobGroupLabels.managers + self.TestExprs.lower_bound_suffix: [0.19] * 2,
                JobGroupLabels.regulated_professions
                + self.TestExprs.lower_bound_suffix: [0.3025] * 2,
                JobGroupLabels.other + self.TestExprs.lower_bound_suffix: [0.4075] * 2,
            },
            schema=schema,
        )
        test_lf = expected_lf.drop(
            JobGroupLabels.direct_care + self.TestExprs.upper_bound_suffix,
            JobGroupLabels.managers + self.TestExprs.upper_bound_suffix,
            JobGroupLabels.regulated_professions + self.TestExprs.upper_bound_suffix,
            JobGroupLabels.other + self.TestExprs.upper_bound_suffix,
            JobGroupLabels.direct_care + self.TestExprs.lower_bound_suffix,
            JobGroupLabels.managers + self.TestExprs.lower_bound_suffix,
            JobGroupLabels.regulated_professions + self.TestExprs.lower_bound_suffix,
            JobGroupLabels.other + self.TestExprs.lower_bound_suffix,
        )
        returned_lf = test_lf.with_columns(
            self.TestExprs.bounds_expressions(
                [0.8, 0.2], self.TestExprs.job_group_cols, self.TestExprs.suffixes
            ),
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
