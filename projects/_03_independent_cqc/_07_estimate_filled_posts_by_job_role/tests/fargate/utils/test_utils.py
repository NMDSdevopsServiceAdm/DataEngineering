import unittest

import polars as pl
import polars.testing as pl_testing

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
)


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


class TestPercentageShare(unittest.TestCase):
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

    def test_when_some_values_are_null(self):
        input_lf = pl.LazyFrame({"vals": [None, 3, None, 2]})
        expected_lf = pl.LazyFrame({"ratios": [None, 0.6, None, 0.4]})
        returned_lf = input_lf.select(job.percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_all_values_are_null(self):
        input_lf = pl.LazyFrame({"vals": [None, None, None]})
        expected_lf = pl.LazyFrame({"ratios": [None, None, None]}).cast(pl.Float64)
        returned_lf = input_lf.select(job.percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_some_values_are_zero(self):
        input_lf = pl.LazyFrame({"vals": [2, 0, 3, 0]})
        # Zero divided by 5 (sum) is still 0.
        expected_lf = pl.LazyFrame({"ratios": [0.4, 0.0, 0.6, 0.0]})
        returned_lf = input_lf.select(job.percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_when_all_values_are_zero(self):
        input_lf = pl.LazyFrame({"vals": [0, 0, 0]})
        # This returns NaN rather than Null because of divide by zero.
        # https://docs.pola.rs/user-guide/expressions/missing-data/#not-a-number-or-nan-values
        expected_lf = pl.LazyFrame({"ratios": [float("nan")] * 3})
        returned_lf = input_lf.select(job.percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_lf, expected_lf)
