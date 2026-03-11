import unittest

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    EstimateIndCqcFilledPostsByJobRoleUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    EstimateIndCqcFilledPostsByJobRoleUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
    MainJobRoleLabels,
)

from .utils_test_cases import rolling_sum_expected_schema, rolling_sum_test_cases


class JoinWorkerToEstimatesDataframeTests(unittest.TestCase):
    def test_join_worker_to_estimates_dataframe_returns_expected_df(self):
        estimates_lf = pl.LazyFrame(
            data=Data.estimates_df_before_join_rows,
            schema=Schemas.estimates_df_before_join_schema,
        )
        worker_lf = pl.LazyFrame(
            data=Data.worker_df_before_join_rows,
            schema=Schemas.worker_df_before_join_schema,
        )
        returned_lf = job.join_worker_to_estimates_dataframe(estimates_lf, worker_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_join_worker_to_estimates_dataframe_rows,
            schema=Schemas.expected_join_worker_to_estimates_dataframe_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


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


@pytest.fixture(
    params=[
        pytest.param(
            (
                [None, 3, None, 2],
                [None, 0.6, None, 0.4],
            ),
            id="when_some_values_are_null",
        ),
        pytest.param(
            (
                [None, None, None, None],
                [None, None, None, None],
            ),
            id="when_all_values_are_null",
        ),
        pytest.param(
            (
                [2, 0, 3, 0],
                [0.4, 0.0, 0.6, 0.0],
            ),
            id="when_some_values_are_zero",
        ),
        pytest.param(
            (
                [0, 0, 0, 0],
                # This returns NaN rather than Null because of divide by zero.
                # https://docs.pola.rs/user-guide/expressions/missing-data/#not-a-number-or-nan-values
                [float("nan")] * 4,
            ),
            id="when_all_values_are_zero",
        ),
    ],
)
def percent_share_edge_cases_test_data(request):
    """Provides data for the percentage share edge cases.

    When using unpack the tuple into `input_` and `expected`.
    """
    return request.param


class TestPercentageShare:
    def test_over_whole_dataset(self):
        input_lf = pl.LazyFrame({"vals": [1, 2, 2]})
        expected_lf = pl.LazyFrame({"ratios": [0.2, 0.4, 0.4]})
        returned_lf = input_lf.select(job.percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_over_groups(self):
        expected_lf = pl.LazyFrame(
            data=[
                ("1", 1, 0.333),
                ("1", 2, 0.667),
                ("2", 2, 0.4),
                ("2", 3, 0.6),
            ],
            schema=["group", "vals", "ratios"],
            orient="row",
        )
        input_lf = expected_lf.select("group", "vals")
        returned_lf = input_lf.with_columns(
            job.percentage_share("vals").over("group").alias("ratios"),
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)

    def test_when_passed_an_expression(self):
        """Test that the function accepts a Polars expression instead of just a string."""
        input_lf = pl.LazyFrame({"vals": [10, 20, 70]})
        # The ratios should remain the same as the underlying distribution is the same.
        expression = pl.col("vals") * 2
        expected_lf = pl.LazyFrame({"ratios": [0.1, 0.2, 0.7]})

        returned_lf = input_lf.select(job.percentage_share(expression).alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_edge_cases(self, percent_share_edge_cases_test_data):
        # Unpack the tuple returned by the fixture
        input_, expected = percent_share_edge_cases_test_data

        input_lf = pl.LazyFrame({"values": input_}).cast(pl.Float64)
        expected_lf = pl.LazyFrame({"output": expected}).cast(pl.Float64)
        returned_lf = input_lf.select(job.percentage_share("values").alias("output"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestPercentageShareHorizontal:
    def test_basic_case(self):
        schema = ["label1", "label2", "label3"]
        input_lf = pl.LazyFrame(
            schema=schema,
            data=[
                [1, 2, 2],
                [1, 4, 5],
            ],
        )
        expected_lf = pl.LazyFrame(
            schema=schema,
            data=[
                [0.2, 0.4, 0.4],
                [0.1, 0.4, 0.5],
            ],
        )
        returned_lf = input_lf.select(job.percentage_share_horizontal(*schema))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_edge_cases(self, percent_share_edge_cases_test_data):
        # Unpack the tuple returned by the fixture
        input_, expected = percent_share_edge_cases_test_data

        schema = ["label1", "label2", "label3", "label4"]
        input_lf = pl.LazyFrame(schema=schema, data=[input_], orient="row").cast(
            pl.Float64
        )
        expected_lf = pl.LazyFrame(schema=schema, data=[expected], orient="row").cast(
            pl.Float64
        )
        returned_lf = input_lf.select(job.percentage_share_horizontal(*schema))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


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
                [1.0, 0.0, 1.0],
                [0.5, 0.0, 0.5],
                id="when_some_values_are_zero",
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


class TestPercentageShareHorizontalHandlingZeroSum:
    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(
                [5.0, 2.0, 1.0],
                [0.625, 0.25, 0.125],
                id="when_all_values_present",
            ),
            pytest.param(
                [0, 0, 0],
                [0.333, 0.333, 0.333],
                id="handles_zero_sum_case_with_even_distribution",
            ),
            pytest.param(
                [1.0, 0.0, 1.0],
                [0.5, 0.0, 0.5],
                id="when_some_values_are_zero",
            ),
        ],
    )
    def test_cases(self, input_, expected):
        schema = ["label1", "label2", "label3"]
        input_lf = pl.LazyFrame(schema=schema, data=[input_], orient="row")
        expected_lf = pl.LazyFrame(schema=schema, data=[expected], orient="row")
        returned_lf = input_lf.select(
            job.percentage_share_horizontal_handling_zero_sum(*schema)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)


class TestImputeFullTimeSeries:
    @pytest.mark.parametrize(
        "input, expected",
        [
            pytest.param([1, None, 3], [1, 2, 3], id="linear_interpolation"),
            pytest.param([None, 1, 3], [1, 1, 3], id="backfill"),
            pytest.param([1, 3, None], [1, 3, 3], id="forward_fill"),
            pytest.param(
                [None, 1, None, 3, None],
                [1, 1, 2, 3, 3],
                id="combined_time_series",
            ),
        ],
    )
    def test_imputations(self, input, expected):
        input_lf = pl.LazyFrame({"vals": input})
        expected_lf = pl.LazyFrame({"vals": expected}).cast(pl.Float64)
        returned_lf = input_lf.select(job.impute_full_time_series("vals"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_all_nones_returns_nones(self):
        """Test for the all None case in a set of values."""
        input_lf = pl.LazyFrame({"vals": [None, None, None, None, None]}).cast(
            pl.Float64
        )
        expected_lf = input_lf
        returned_lf = input_lf.select(job.impute_full_time_series("vals"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_imputes_time_series_over_groups_with_unordered_time_col(self):
        """Test that it works with `.over(groups)` ordering by a time column."""
        input_lf = pl.LazyFrame(
            schema=["group", "time_col", "vals"],
            data=[
                # Scrambled the order of time_col to test order by.
                ("a", 4, 0.3),
                ("a", 1, None),
                ("a", 3, None),
                ("a", 2, 0.1),
                ("a", 5, None),
                ("b", 2, None),
                ("b", 3, 0.2),
                ("b", 1, 0.1),
            ],
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            schema=["group", "time_col", "vals"],
            data=[
                ("a", 1, 0.1),
                ("a", 2, 0.1),
                ("a", 3, 0.2),
                ("a", 4, 0.3),
                ("a", 5, 0.3),
                ("b", 1, 0.1),
                ("b", 2, 0.15),
                ("b", 3, 0.2),
            ],
            orient="row",
        )
        returned_lf = input_lf.with_columns(
            # Overwriting the original column with output
            job.impute_full_time_series("vals").over("group", order_by="time_col")
        )
        # `.over()` will return rows in original order, so need to sort to match expected.
        pl_testing.assert_frame_equal(
            returned_lf.sort("group", "time_col"),
            expected_lf,
        )


class TestRollingSum:
    @pytest.mark.parametrize(
        "rolling_sum_data",
        [pytest.param(case.data, id=case.id) for case in rolling_sum_test_cases],
    )
    def test_rolling_sum(self, rolling_sum_data):
        expected_lf = pl.LazyFrame(
            rolling_sum_data, rolling_sum_expected_schema, orient="row"
        )
        input_lf = expected_lf.drop(expected_lf.columns[-1])
        returned_lf = input_lf.with_columns(
            job.rolling_sum_of_job_role_counts(period="6mo").alias(
                IndCQC.ascwds_job_role_rolling_sum
            )
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


@pytest.mark.parametrize(
    "input_, expected",
    [
        pytest.param(
            [["Sarah", "James"], ["Matt"], []],
            [True, True, False],
            id="empty_list_to_False",
        ),
        pytest.param(
            [["Sarah", "James"], None, None],
            [True, None, None],
            id="null_case",
        ),
    ],
)
def test_has_elements(input_, expected):
    input_lf = pl.LazyFrame({"names": input_})
    expected_lf = pl.LazyFrame({"has_elements": expected})
    returned_lf = input_lf.select(job.has_elements("names").alias("has_elements"))
    pl_testing.assert_frame_equal(returned_lf, expected_lf)


@pytest.mark.parametrize(
    "input_, expected",
    [
        pytest.param(["Sarah", "James"], 1, id="more_than_1_capped_to_1"),
        pytest.param(["Sarah"], 1, id="1_stays_1"),
        pytest.param([], 0, id="empty_list_to_0"),
        pytest.param(None, 0, id="null_to_0"),
    ],
)
def test_cap_registered_managers_to_1(input_, expected):
    schema = {
        IndCQC.registered_manager_names: pl.List,
        IndCQC.registered_manager_count: pl.Int8,
    }
    expected_lf = pl.LazyFrame([[input_], [expected]], schema=schema)
    input_lf = expected_lf.drop(IndCQC.registered_manager_count)
    returned_lf = input_lf.with_columns(
        job.cap_registered_managers_to_1().alias(IndCQC.registered_manager_count)
    )
    pl_testing.assert_frame_equal(returned_lf, expected_lf)


def test_get_estimated_managers_diff_from_cqc_registered_managers():
    output_col = IndCQC.difference_between_estimate_and_cqc_registered_managers
    schema = [
        IndCQC.registered_manager_names,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.estimate_filled_posts,
        output_col,
    ]
    # The output col here should be the filled_posts value for
    # "registered_manager" minus the registered_manager_names count (3 - 1 = 2)
    # broadcast across the remaining rows.
    data = [
        (["Sarah"], MainJobRoleLabels.registered_manager, 3, 2),
        (["Sarah"], MainJobRoleLabels.social_worker, 7, 2),
        (["Sarah"], MainJobRoleLabels.care_worker, 12, 2),
        (["Sarah"], MainJobRoleLabels.supervisor, 4, 2),
    ]
    expected_lf = pl.LazyFrame(data=data, schema=schema, orient="row")  # fmt: skip
    input_lf = expected_lf.drop(output_col)
    returned_lf = input_lf.with_columns(
        job.get_estimated_managers_diff_from_cqc_registered_managers().alias(output_col)
    )
    pl_testing.assert_frame_equal(returned_lf, expected_lf)
