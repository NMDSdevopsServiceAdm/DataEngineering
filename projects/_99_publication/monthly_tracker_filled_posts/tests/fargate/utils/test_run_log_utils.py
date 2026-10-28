from types import SimpleNamespace
from unittest.mock import Mock, patch

import polars as pl
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.run_log_utils as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate.utils.run_log_utils"
)
RUN_LOG_SOURCE = "s3://bucket/run_log/"


def _run_log_lf(published: dict[int, bool]) -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            ArchiveKeys.run_number: list(published),
            RunLog.selected_for_publication: list(published.values()),
        },
        schema={
            ArchiveKeys.run_number: pl.Int64,
            RunLog.selected_for_publication: pl.Boolean,
        },
    )


def _run_row(promoted: bool = True, reconciled: bool = False) -> pl.DataFrame:
    return pl.DataFrame(
        {
            RunLog.approved: [True],
            RunLog.selected_for_publication: [True],
            RunLog.promoted: [promoted],
            RunLog.reconciled: [reconciled],
            RunLog.reconciled_at: [None],
        },
        schema={
            RunLog.approved: pl.Boolean,
            RunLog.selected_for_publication: pl.Boolean,
            RunLog.promoted: pl.Boolean,
            RunLog.reconciled: pl.Boolean,
            RunLog.reconciled_at: pl.Datetime,
        },
    )


PARTITION_PREFIX = "run_log/archive_date=2026-10-06/run_number=1/"


@pytest.fixture
def mocks():
    """Mocks the S3 reads and writes around a run log partition holding file.parquet."""
    with (
        patch(f"{PATCH_PATH}.file_utils.list_archive_runs") as list_archive_runs,
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_parquet,
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_to_parquet,
        patch(f"{PATCH_PATH}.boto3.client") as boto3_client,
    ):
        list_archive_runs.return_value = {1: "2026-10-06"}
        scan_parquet.return_value = _run_row().lazy()
        boto3_client.return_value.list_objects_v2.return_value = {
            "Contents": [{"Key": f"{PARTITION_PREFIX}file.parquet"}]
        }
        yield SimpleNamespace(
            scan_parquet=scan_parquet,
            sink_to_parquet=sink_to_parquet,
            list_objects_v2=boto3_client.return_value.list_objects_v2,
        )


class TestMarkRunLogFlag:
    def test_sets_the_flag_and_overwrites_the_partition(self, mocks):
        uploaded = {}
        mocks.sink_to_parquet.side_effect = (
            lambda lazy_df, destination: uploaded.update(
                frame=lazy_df.collect(), destination=destination
            )
        )

        job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        mocks.list_objects_v2.assert_called_once_with(
            Bucket="bucket", Prefix=PARTITION_PREFIX
        )
        assert uploaded["destination"] == f"s3://bucket/{PARTITION_PREFIX}"
        assert uploaded["frame"][RunLog.reconciled].to_list() == [True]

    def test_refuses_to_write_when_the_partition_holds_other_files(self, mocks):
        mocks.list_objects_v2.return_value = {
            "Contents": [
                {"Key": f"{PARTITION_PREFIX}00000000.parquet"},
                {"Key": f"{PARTITION_PREFIX}file.parquet"},
            ]
        }

        with pytest.raises(ValueError, match="file.parquet"):
            job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        mocks.sink_to_parquet.assert_not_called()

    def test_does_not_upload_when_the_update_is_rejected(self, mocks):
        mocks.scan_parquet.return_value = _run_row(promoted=False).lazy()

        with pytest.raises(ValueError, match="not promoted"):
            job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        mocks.sink_to_parquet.assert_not_called()

    def test_does_nothing_when_the_flag_is_already_set(self, mocks):
        mocks.scan_parquet.return_value = _run_row(reconciled=True).lazy()

        job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        mocks.sink_to_parquet.assert_not_called()


class TestGetPreviousPublishedRunNumber:
    @pytest.mark.parametrize(
        "published, run_number, expected",
        [
            ({1: True, 3: True, 5: False, 6: False}, 6, 3),
            ({1: True, 2: False, 3: False}, 3, 1),
            ({1: True, 4: True, 5: True}, 4, 1),
            ({1: False, 2: False}, 3, None),
            ({3: True}, 3, None),
            ({}, 2, None),
        ],
        ids=[
            "returns_highest_published_run_below_current",
            "ignores_unpublished_runs",
            "ignores_published_runs_at_or_above_current",
            "returns_none_when_no_run_is_published",
            "returns_none_when_only_published_run_is_current",
            "returns_none_when_run_log_is_empty",
        ],
    )
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_returns_expected_run_number(
        self,
        scan_parquet_mock: Mock,
        published: dict[int, bool],
        run_number: int,
        expected: int | None,
    ):
        scan_parquet_mock.return_value = _run_log_lf(published)

        returned = job.get_previous_published_run_number(RUN_LOG_SOURCE, run_number)

        assert returned == expected
        assert scan_parquet_mock.call_args.args == (RUN_LOG_SOURCE,)
