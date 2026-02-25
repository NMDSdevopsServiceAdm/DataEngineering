import unittest
from typing import Any

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
        # fmt: off
        input_rows = [
            ("1-001", 10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.care_worker, 1),
            ("1-001", 10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.registered_nurse, 2),
            ("1-002", None, 20.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.care_worker, 1),
            ("1-003", 10.0, 10.0, EstimateFilledPostsSource.care_home_model, MainJobRoleLabels.registered_nurse, 2),
        ]

        expected_rows = [
            ("1-001", 10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.care_worker, 1),
            ("1-001", 10.0, 10.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.registered_nurse, 2),
            ("1-002", None, 20.0, EstimateFilledPostsSource.ascwds_pir_merged, MainJobRoleLabels.care_worker, None),
            ("1-003", 10.0, 10.0, EstimateFilledPostsSource.care_home_model, MainJobRoleLabels.registered_nurse, None),
        ]
        # fmt: on
        test_schema = pl.Schema(
            [
                (IndCQC.location_id, pl.String()),
                (IndCQC.ascwds_filled_posts_dedup_clean, pl.Float64()),
                (IndCQC.estimate_filled_posts, pl.Float64()),
                (IndCQC.estimate_filled_posts_source, pl.String()),
                (IndCQC.main_job_role_clean_labelled, pl.String()),
                (IndCQC.ascwds_job_role_counts, pl.Int64()),
            ]
        )
        # This function shouldn't change the schema from input.
        self.input_lf = pl.LazyFrame(input_rows, test_schema, orient="row")
        self.expected_lf = pl.LazyFrame(expected_rows, test_schema, orient="row")

        return super().setUp()

    def test_nullify_job_role_count_when_source_not_ascwds(self):
        returned_lf = job.nullify_job_role_count_when_source_not_ascwds(self.input_lf)
        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)


class TestGetPercentageShare(unittest.TestCase):
    def test_over_whole_dataset(self):
        input_df = pl.DataFrame({"vals": [1, 2, 2]})
        expected_df = pl.DataFrame({"ratios": [0.2, 0.4, 0.4]})
        returned_df = input_df.select(job.get_percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_over_groups(self):
        expected_df = pl.DataFrame(
            data=[
                ("1", 1, 0.333),
                ("1", 2, 0.667),
                ("2", 2, 0.4),
                ("2", 3, 0.6),
            ],
            schema=["group", "vals", "ratios"],
        )
        input_df = expected_df.select("group", "vals")
        returned_df = input_df.with_columns(
            job.get_percentage_share("vals").over("group").alias("ratios"),
        )
        pl_testing.assert_frame_equal(returned_df, expected_df, rel_tol=1e-03)

    def test_when_all_values_are_null(self):
        input_df = pl.DataFrame({"vals": [None, None, None]})
        expected_df = pl.DataFrame({"ratios": [None, None, None]}).cast(pl.Float64)
        returned_df = input_df.select(job.get_percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_when_some_values_are_null(self):
        input_df = pl.DataFrame({"vals": [None, 3, None, 2]})
        expected_df = pl.DataFrame({"ratios": [None, 0.6, None, 0.4]})
        returned_df = input_df.select(job.get_percentage_share("vals").alias("ratios"))
        pl_testing.assert_frame_equal(returned_df, expected_df)


def get_test_dfs(
    expected_data: list[tuple[Any]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Construct input and expected DataFrames or interpolate and extrapolate tests."""
    # Remove the output column from the expected data to get input data.
    input_data = [row[:-1] for row in expected_data]
    input_schema = [
        IndCQC.location_id,
        IndCQC.unix_time,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.ascwds_job_role_ratios,
    ]
    expected_schema = input_schema + ["output"]
    input_df = pl.DataFrame(input_data, schema=input_schema, orient="row")
    expected_df = pl.DataFrame(expected_data, schema=expected_schema, orient="row")
    return input_df, expected_df


class TestInterpolate:
    @pytest.mark.parametrize(
        "expected_data",
        [
            [
                ("1000", 1000, MainJobRoleLabels.care_worker, 1.0, 1.0),
                ("1000", 1000, MainJobRoleLabels.registered_nurse, 1.0, 1.0),
                ("1000", 1000, MainJobRoleLabels.senior_care_worker, 1.0, 1.0),
                ("1000", 1000, MainJobRoleLabels.senior_management, 1.0, 1.0),
                ("1000", 1001, MainJobRoleLabels.care_worker, None, 2.0),
                ("1000", 1001, MainJobRoleLabels.registered_nurse, None, 2.0),
                ("1000", 1001, MainJobRoleLabels.senior_care_worker, None, 2.0),
                ("1000", 1001, MainJobRoleLabels.senior_management, None, 2.0),
                ("1000", 1002, MainJobRoleLabels.care_worker, 3.0, 3.0),
                ("1000", 1002, MainJobRoleLabels.registered_nurse, 3.0, 3.0),
                ("1000", 1002, MainJobRoleLabels.senior_care_worker, 3.0, 3.0),
                ("1000", 1002, MainJobRoleLabels.senior_management, 3.0, 3.0),
            ],
            [
                ("1000", 1000, MainJobRoleLabels.care_worker, None, None),
                ("1000", 1000, MainJobRoleLabels.registered_nurse, None, None),
                ("1000", 1001, MainJobRoleLabels.care_worker, 2.0, 2.0),
                ("1000", 1001, MainJobRoleLabels.registered_nurse, 4.0, 4.0),
                ("1000", 1002, MainJobRoleLabels.care_worker, None, 1.0),
                ("1000", 1002, MainJobRoleLabels.registered_nurse, None, 3.0),
                ("1000", 1003, MainJobRoleLabels.care_worker, 0.0, 0.0),
                ("1000", 1003, MainJobRoleLabels.registered_nurse, 2.0, 2.0),
                ("1000", 1004, MainJobRoleLabels.care_worker, None, None),
                ("1000", 1004, MainJobRoleLabels.registered_nurse, None, None),
            ],
        ],
        ids=["interpolate_in_between", "nones_at_start_and_end"],
    )
    def test_linear_interpolation(self, expected_data):
        # Setup.
        input_df, expected_df = get_test_dfs(expected_data)
        # Do test.
        returned_df = input_df.with_columns(
            pl.col(IndCQC.ascwds_job_role_ratios)
            .interpolate()
            .over(
                IndCQC.location_id,
                IndCQC.main_job_role_clean_labelled,
                order_by=IndCQC.unix_time,
            )
            .alias("output")
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)


class TestExtrapolate:
    """Extrapolating in our case is filling forwards/backwards within groups ordered by time."""

    def test_fill_forwards_and_backwards(self):
        expected_data = [
            ("1-001", 1000000200, MainJobRoleLabels.care_worker, None, 0.1),
            ("1-001", 1000000200, MainJobRoleLabels.registered_nurse, None, 0.1),
            ("1-001", 1000000300, MainJobRoleLabels.care_worker, 0.1, 0.1),
            ("1-001", 1000000300, MainJobRoleLabels.registered_nurse, 0.1, 0.1),
            ("1-001", 1000000400, MainJobRoleLabels.care_worker, 0.2, 0.2),
            ("1-001", 1000000400, MainJobRoleLabels.registered_nurse, 0.2, 0.2),
            ("1-001", 1000000500, MainJobRoleLabels.care_worker, 0.3, 0.3),
            ("1-001", 1000000500, MainJobRoleLabels.registered_nurse, 0.3, 0.3),
            ("1-001", 1000000600, MainJobRoleLabels.care_worker, None, 0.3),
            ("1-001", 1000000600, MainJobRoleLabels.registered_nurse, None, 0.3),
            ("1-002", 1000000200, MainJobRoleLabels.care_worker, 0.1, 0.1),
            ("1-002", 1000000200, MainJobRoleLabels.registered_nurse, 0.1, 0.1),
            ("1-002", 1000000300, MainJobRoleLabels.care_worker, None, 0.15),
            ("1-002", 1000000300, MainJobRoleLabels.registered_nurse, None, 0.15),
            ("1-002", 1000000400, MainJobRoleLabels.care_worker, 0.2, 0.2),
            ("1-002", 1000000400, MainJobRoleLabels.registered_nurse, 0.2, 0.2),
            ("1-003", 1000000200, MainJobRoleLabels.care_worker, None, None),
            ("1-003", 1000000200, MainJobRoleLabels.registered_nurse, None, None),
            ("1-003", 1000000300, MainJobRoleLabels.care_worker, None, None),
            ("1-003", 1000000300, MainJobRoleLabels.registered_nurse, None, None),
            ("1-003", 1000000400, MainJobRoleLabels.care_worker, None, None),
            ("1-003", 1000000400, MainJobRoleLabels.registered_nurse, None, None),
            ("1-003", 1000000500, MainJobRoleLabels.care_worker, None, None),
            ("1-003", 1000000500, MainJobRoleLabels.registered_nurse, None, None),
        ]
        input_df, expected_df = get_test_dfs(expected_data)
        # Do test.
        returned_df = input_df.with_columns(
            pl.col(IndCQC.ascwds_job_role_ratios)
            .interpolate()
            .forward_fill()
            .backward_fill()
            .over(
                IndCQC.location_id,
                IndCQC.main_job_role_clean_labelled,
                order_by=IndCQC.unix_time,
            )
            .alias("output")
        )
        pl_testing.assert_frame_equal(returned_df, expected_df)
