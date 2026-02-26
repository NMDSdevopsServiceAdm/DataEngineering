import unittest
from unittest.mock import ANY, MagicMock, call

import polars as pl
import polars.testing as pl_testing
import pytest

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

ESTIMATE_SOURCE = "some/source"
PREPARED_JOB_ROLE_COUNTS_SOURCE = "some/other/source"
ESTIMATES_DESTINATION = "some/destination"


@pytest.fixture
def mock_lf():
    """Factory to create a mock that handles Polars .pipe() behavior."""

    def _create_mock(name):
        return MagicMock(wraps=pl.LazyFrame(), name=name)

    return _create_mock


def test_main_runs(monkeypatch, mock_lf):
    flow_mock_lf = mock_lf("estimates_lf")

    # Side effect will return a different value on each call
    mock_scan = MagicMock(side_effect=[flow_mock_lf, MagicMock(name="ascwds_data")])
    # Define return values as the flow_mock_lf
    mock_join = MagicMock(return_value=flow_mock_lf)
    mock_nullify = MagicMock(return_value=flow_mock_lf)
    mock_sink = MagicMock()

    # Apply monkeypatches
    monkeypatch.setattr(job.utils, "scan_parquet", mock_scan)
    monkeypatch.setattr(job, "join_worker_to_estimates_dataframe", mock_join)
    monkeypatch.setattr(
        job, "nullify_job_role_count_when_source_not_ascwds", mock_nullify
    )
    monkeypatch.setattr(job.utils, "sink_to_parquet", mock_sink)

    # Execution
    job.main(ESTIMATE_SOURCE, PREPARED_JOB_ROLE_COUNTS_SOURCE, ESTIMATES_DESTINATION)

    # Verify scan_parquet was called twice
    assert mock_scan.call_count == 2
    mock_scan.assert_has_calls(
        [
            call(
                source=ESTIMATE_SOURCE, selected_columns=job.estimates_columns_to_import
            ),
            call(
                source=PREPARED_JOB_ROLE_COUNTS_SOURCE,
                selected_columns=job.ascwds_columns_to_import,
            ),
        ]
    )
    mock_join.assert_called_once()
    mock_nullify.assert_called_once()
    mock_sink.assert_called_once_with(
        lazy_df=flow_mock_lf,
        output_path=ESTIMATES_DESTINATION,
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
