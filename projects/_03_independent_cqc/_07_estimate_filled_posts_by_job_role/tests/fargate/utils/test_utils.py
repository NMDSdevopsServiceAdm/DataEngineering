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
        self.input_lf = pl.LazyFrame(input_rows, test_schema)
        self.expected_lf = pl.LazyFrame(expected_rows, test_schema)

        return super().setUp()

    def test_nullify_job_role_count_when_source_not_ascwds(self):
        returned_lf = job.nullify_job_role_count_when_source_not_ascwds(self.input_lf)
        pl_testing.assert_frame_equal(returned_lf, self.expected_lf)


class GetJobRolePercentShareOfTotal(unittest.TestCase):
    def setUp(self) -> None:
        input_rows = [
            ("1-001", 1),
            ("1-001", 2),
            ("1-002", None),
            ("1-002", None),
            ("1-003", None, None),
        ]
        input_schema = {
            IndCQC.location_id: pl.String,
            IndCQC.ascwds_job_role_counts: pl.Int64,
        }

        expected_rows = [
            ("1-001", 1, 0.333),
            ("1-001", 2, 0.667),
            ("1-002", None, None),
            ("1-002", None, None),
            ("1-003", None, None, None),
        ]
        expected_schema = input_schema.copy()
        expected_schema.update({"ratios": pl.Float64})

        self.input_lf = pl.LazyFrame(input_rows, input_schema)
        self.expected_lf = pl.LazyFrame(expected_rows, expected_schema)

        return super().setUp()

    def test_get_pct_share_over_group_columns(self):
        returned_lf = self.input_lf.with_columns(
            job.get_percentage_share(IndCQC.ascwds_job_role_counts)
            .over(IndCQC.location_id)
            .alias("ratios")
        )
        pl_testing.assert_frame_equal(returned_lf, self.expected_lf, rel_tol=1e-03)
