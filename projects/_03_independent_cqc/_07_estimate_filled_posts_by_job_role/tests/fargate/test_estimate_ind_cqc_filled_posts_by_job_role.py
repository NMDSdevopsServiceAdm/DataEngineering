import unittest
from unittest.mock import ANY, Mock, call, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role as job
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

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.estimate_ind_cqc_filled_posts_by_job_role"


class MainTests(unittest.TestCase):
    ESTIMATE_SOURCE = "some/source"
    PREPARED_JOB_ROLE_COUNTS_SOURCE = "some/other/source"
    ESTIMATES_DESTINATION = "some/destination"

    mock_estimate_data = Mock(name="estimate_data")
    mock_prepared_job_role_counts_data = Mock(name="prepared_job_role_counts_data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.join_worker_to_estimates_dataframe")
    @patch(f"{PATCH_PATH}.nullify_job_role_count_when_source_not_ascwds")
    @patch(
        f"{PATCH_PATH}.utils.scan_parquet",
        return_value=[mock_estimate_data, mock_prepared_job_role_counts_data],
    )
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        join_worker_to_estimates_dataframe_mock: Mock,
        nullify_job_role_count_when_source_not_ascwds_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.ESTIMATE_SOURCE,
            self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
            self.ESTIMATES_DESTINATION,
        )

        self.assertEqual(scan_parquet_mock.call_count, 2)
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    source=self.ESTIMATE_SOURCE,
                    selected_columns=job.estimates_columns_to_import,
                ),
                call(
                    source=self.PREPARED_JOB_ROLE_COUNTS_SOURCE,
                    selected_columns=job.ascwds_columns_to_import,
                ),
            ]
        )

        join_worker_to_estimates_dataframe_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.ESTIMATES_DESTINATION,
            partition_cols=job.partition_keys,
            append=False,
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
