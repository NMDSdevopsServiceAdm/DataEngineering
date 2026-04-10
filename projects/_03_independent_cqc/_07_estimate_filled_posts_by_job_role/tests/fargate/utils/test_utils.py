import unittest
from datetime import date
from typing import Final
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EstimateFilledPostsSource

from .utils_test_cases import (
    create_ascwds_job_role_rolling_ratio_expected_schema,
    create_ascwds_job_role_rolling_ratio_test_cases,
    managerial_adjustment_core_schema,
    managerial_adjustment_expected_schema,
    managerial_adjustment_grouping_test_data,
    managerial_adjustment_test_cases,
)

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils"

ROW_ID: Final[str] = "id"
EXPANDED_ID: Final[str] = "expanded_id"


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

        self.expected_rows_that_meet_condition = [
            (10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, 1),
            (10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, 2),
        ]

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


# Check test cases still work
class TestGetPercentageShareRatios:
    def test_over_groups(self):
        expected_lf = pl.LazyFrame(
            data=[
                (1, "1", date(2026, 1, 1), 1, 0.3333),
                (2, "1", date(2026, 1, 1), 2, 0.6667),
                (3, "1", date(2026, 2, 1), 2, 0.5),
                (4, "1", date(2026, 2, 1), 2, 0.5),
                (5, "2", date(2026, 1, 1), 2, 0.4),
                (6, "2", date(2026, 1, 1), 3, 0.6),
            ],
            schema={
                EXPANDED_ID: pl.Int64,
                IndCQC.location_id: pl.String,
                IndCQC.cqc_location_import_date: pl.Date,
                "vals": pl.Int64,
                "ratios": pl.Float32,
            },
            orient="row",
        )
        input_lf = expected_lf.drop("ratios")
        returned_lf = job.get_percent_share_ratios(
            input_lf, input_col="vals", output_col="ratios"
        ).sort(EXPANDED_ID)
        returned_lf.show(limit=6)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)


class TestPercentageShareHandlingZeroSum:
    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(
                [5.0, 2.0, 1.0],
                [0.625, 0.25, 0.125],
                id="when_all_values_present",
            ),
            pytest.param(
                [0, 0],
                [0.5, 0.5],
                id="handles_zero_sum_case_with_even_distribution",
            ),
            pytest.param(
                [0, None, 0, None],
                [0.5, None, 0.5, None],
                id="handles_zero_sum_case_with_even_distribution_across_non_nulls",
            ),
        ],
    )
    def test_percentage_share_handling_zero_sum(self, input_, expected):
        input_lf = pl.LazyFrame({"values": input_})
        expected_lf = pl.LazyFrame({"pct_share": expected})
        returned_lf = input_lf.select(
            job.percentage_share_handling_zero_sum("values").alias("pct_share")
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


# Needs repointing at new function- check test cases still work
class TestCreateImputedASCWDSJobRoleCounts(unittest.TestCase):
    schema = {
        EXPANDED_ID: pl.UInt32,
        IndCQC.location_id: pl.String,
        IndCQC.main_job_role_clean_labelled: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_job_role_counts: pl.Int64,
        IndCQC.estimate_filled_posts: pl.Float32,
        IndCQC.ascwds_job_role_ratios: pl.Float32,  # extra col
        IndCQC.imputed_ascwds_job_role_ratios: pl.Float32,  # extra col
        IndCQC.imputed_ascwds_job_role_counts: pl.Float32,
    }

    def _create_expected_lf(
        self, expected_data: list[tuple], schema: dict
    ) -> pl.LazyFrame:
        """Set the expected LazyFrame up"""
        return pl.LazyFrame(expected_data, schema, orient="row")

    def _create_test_lf(self, expected_lf: pl.LazyFrame) -> pl.LazyFrame:
        """Set the test LazyFrame up"""
        return expected_lf.drop(
            IndCQC.imputed_ascwds_job_role_counts,
            IndCQC.ascwds_job_role_ratios,
            IndCQC.imputed_ascwds_job_role_ratios,
        )

    def test_imputations(self):
        expected_data = [
            ("1", "1", "job_role_a", date(2026, 1, 1), 1, 1.0, 1.0, 1.0, 1.0),
            ("2", "1", "job_role_a", date(2026, 1, 2), None, 2.0, None, 0.7, 1.4),
            ("3", "1", "job_role_a", date(2026, 1, 3), 4, 4.0, 0.4, 0.4, 1.6),
            ("4", "1", "job_role_b", date(2026, 1, 1), None, 1.0, None, 1.0, 1.0),
            ("5", "1", "job_role_b", date(2026, 1, 2), 2, 2.0, 1.0, 1.0, 2.0),
            ("6", "1", "job_role_b", date(2026, 1, 3), 6, 6.0, 0.6, 0.6, 3.6),
            ("7", "2", "job_role_a", date(2026, 1, 1), 1, 1.0, 1.0, 1.0, 1.0),
            ("8", "2", "job_role_a", date(2026, 1, 2), 9, 9.0, 1.0, 1.0, 9.0),
            ("9", "2", "job_role_a", date(2026, 1, 3), None, 1.0, None, 1.0, 1.0),
        ]
        expected_lf = self._create_expected_lf(expected_data, self.schema)
        test_lf = self._create_test_lf(expected_lf)
        returned_lf = job.create_imputed_ascwds_job_role_counts(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_all_nones_returns_nones(self):
        """Test for the all None case in a set of values."""
        expected_data = [
            ("1", "1", "job_role_a", date(2026, 1, 1), None, 1.0, None, None, None),
            ("2", "1", "job_role_a", date(2026, 1, 2), None, 2.0, None, None, None),
            ("3", "1", "job_role_a", date(2026, 1, 3), None, 4.0, None, None, None),
            ("4", "1", "job_role_b", date(2026, 1, 1), None, 1.0, None, None, None),
            ("5", "1", "job_role_b", date(2026, 1, 2), None, 2.0, None, None, None),
            ("6", "1", "job_role_b", date(2026, 1, 3), None, 6.0, None, None, None),
            ("7", "2", "job_role_a", date(2026, 1, 1), None, 1.0, None, None, None),
            ("8", "2", "job_role_a", date(2026, 1, 2), None, 9.0, None, None, None),
            ("9", "2", "job_role_a", date(2026, 1, 3), None, 1.0, None, None, None),
        ]
        expected_lf = self._create_expected_lf(expected_data, self.schema)
        test_lf = self._create_test_lf(expected_lf)
        returned_lf = job.create_imputed_ascwds_job_role_counts(test_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


# Update test data and test cases
class TestCreateASCWDSJobRoleRollingRatio:
    @pytest.mark.parametrize(
        "rolling_sum_data",
        [
            case.as_pytest_param()
            for case in create_ascwds_job_role_rolling_ratio_test_cases
        ],
    )
    def test_create_ascwds_job_role_rolling_ratio(self, rolling_sum_data):
        expected_lf = pl.LazyFrame(
            rolling_sum_data,
            create_ascwds_job_role_rolling_ratio_expected_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.ascwds_job_role_rolling_sum, IndCQC.ascwds_job_role_rolling_ratio
        )
        returned_lf = job.create_ascwds_job_role_rolling_ratio(input_lf)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.0001)


class TestManagerialFilledPostAdjustmentExpr:
    @pytest.fixture(
        params=[case.as_pytest_param() for case in managerial_adjustment_test_cases]
    )
    def input_data(self, request):
        """Provides 4 different test cases to put through the managerial adjustment tests.

        The test data comes with three different output columns: "diff",
        "proportions", "adjusted_estimates". The right one should be selected for
        each expected_lf.

        """
        return request.param

    @pytest.fixture
    def expected_lf_constructor(self, input_data):
        """A helper fixture to construct the expected_lf given output_col."""

        def inner(output_col: str) -> pl.LazyFrame:
            return pl.LazyFrame(
                data=input_data,
                schema=managerial_adjustment_expected_schema,
                orient="row",
            ).select(*managerial_adjustment_core_schema.keys(), output_col)

        return inner

    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(["Sarah", "James"], 1, id="more_than_1_capped_to_1"),
            pytest.param(["Sarah"], 1, id="1_stays_1"),
            pytest.param([], 0, id="empty_list_to_0"),
            pytest.param(None, 0, id="null_to_0"),
        ],
    )
    def test_clip_rm_count(self, input_, expected):
        schema = {
            IndCQC.registered_manager_names: pl.List,
            IndCQC.registered_manager_count: pl.UInt32,
        }
        expected_lf = pl.LazyFrame([[input_], [expected]], schema=schema)
        input_lf = expected_lf.drop(IndCQC.registered_manager_count)
        clip_count_expr = job.ManagerialFilledPostAdjustmentExpr._clip_rm_count()
        returned_lf = input_lf.with_columns(
            clip_count_expr.alias(IndCQC.registered_manager_count)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.fixture(
        params=[
            {"method": "_rm_manager_diff", "col": "diff"},
            {"method": "_non_rm_manager_proportions", "col": "proportions"},
            {"method": "build", "col": "adjusted_estimates"},
        ],
        ids=lambda d: d["method"].lstrip("_"),
    )
    def config_data(self, request):
        return request.param

    def test_expr_methods(self, expected_lf_constructor, config_data):
        output_col = config_data["col"]
        expected_lf = expected_lf_constructor(output_col)
        input_lf = expected_lf.drop(output_col)
        expr_method = getattr(
            job.ManagerialFilledPostAdjustmentExpr, config_data["method"]
        )
        returned_lf = input_lf.with_columns(expr_method().alias(output_col))
        pl_testing.assert_frame_equal(returned_lf, expected_lf, abs_tol=0.001)

    def test_build_expr_over_groups(self):
        """Test by grouping over location_id and cqc_location_import_date."""
        output_col = "adjusted_estimates"
        expected_lf = pl.LazyFrame(
            data=managerial_adjustment_grouping_test_data,
            schema=managerial_adjustment_expected_schema,
            orient="row",
        ).select(*managerial_adjustment_core_schema.keys(), output_col)

        input_lf = expected_lf.drop(output_col)
        returned_lf = input_lf.with_columns(
            job.ManagerialFilledPostAdjustmentExpr.build()
            .over(IndCQC.location_id, IndCQC.cqc_location_import_date)
            .alias(output_col)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, abs_tol=0.001)
