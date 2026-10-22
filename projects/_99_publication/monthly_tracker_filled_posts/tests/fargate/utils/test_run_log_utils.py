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
