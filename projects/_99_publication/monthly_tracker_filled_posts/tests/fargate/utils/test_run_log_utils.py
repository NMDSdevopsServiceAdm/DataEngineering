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


def _run_row(reconciled: bool = False, reconciled_at=None) -> pl.DataFrame:
    return pl.DataFrame(
        {
            RunLog.approved: [True],
            RunLog.selected_for_publication: [True],
            RunLog.promoted: [True],
            RunLog.reconciled: [reconciled],
            RunLog.reconciled_at: [reconciled_at],
        },
        schema={
            RunLog.approved: pl.Boolean,
            RunLog.selected_for_publication: pl.Boolean,
            RunLog.promoted: pl.Boolean,
            RunLog.reconciled: pl.Boolean,
            RunLog.reconciled_at: pl.Datetime,
        },
    )


class TestMarkRunLogFlag:
    @patch(f"{PATCH_PATH}.file_utils.empty_s3_folder")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_downloads_sets_and_reuploads_the_target_partition(
        self,
        list_archive_runs_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        empty_s3_folder_mock: Mock,
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-06"}
        scan_parquet_mock.return_value = _run_row().lazy()
        uploaded = {}
        sink_to_parquet_mock.side_effect = lambda lazy_df, destination: uploaded.update(
            frame=lazy_df.collect(), destination=destination
        )

        job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        empty_s3_folder_mock.assert_called_once_with(
            "bucket", "run_log/archive_date=2026-10-06/run_number=1/"
        )
        assert (
            uploaded["destination"]
            == "s3://bucket/run_log/archive_date=2026-10-06/run_number=1/"
        )
        assert uploaded["frame"][RunLog.reconciled].to_list() == [True]

    @patch(f"{PATCH_PATH}.file_utils.empty_s3_folder")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_clears_the_destination_partition_before_reuploading(
        self,
        list_archive_runs_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        empty_s3_folder_mock: Mock,
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-06"}
        scan_parquet_mock.return_value = _run_row().lazy()
        call_order = []
        empty_s3_folder_mock.side_effect = lambda *a, **k: call_order.append("empty")
        sink_to_parquet_mock.side_effect = lambda *a, **k: call_order.append("sink")

        job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        assert call_order == ["empty", "sink"]

    @patch(f"{PATCH_PATH}.file_utils.empty_s3_folder")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.file_utils.list_archive_runs")
    def test_does_not_upload_when_the_update_is_rejected(
        self,
        list_archive_runs_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        empty_s3_folder_mock: Mock,
    ):
        list_archive_runs_mock.return_value = {1: "2026-10-06"}
        scan_parquet_mock.return_value = _run_row(reconciled=True).lazy()

        with pytest.raises(ValueError, match="already"):
            job.mark_run_log_flag(RUN_LOG_SOURCE, 1, RunLog.reconciled)

        empty_s3_folder_mock.assert_not_called()
        sink_to_parquet_mock.assert_not_called()


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
