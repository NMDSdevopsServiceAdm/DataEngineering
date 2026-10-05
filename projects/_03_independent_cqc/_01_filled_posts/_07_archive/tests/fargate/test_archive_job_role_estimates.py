from datetime import date, datetime
from unittest.mock import Mock, call, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._01_filled_posts._07_archive.fargate.archive_job_role_estimates as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLogCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._01_filled_posts._07_archive.fargate.archive_job_role_estimates"

ESTIMATES_SOURCE = "some/estimates/directory"
METADATA_SOURCE = "some/metadata/directory"
ESTIMATES_DESTINATION = "some/estimates/destination"
METADATA_DESTINATION = "some/metadata/destination"
RUN_LOG_DESTINATION = "some/run_log/destination"
MAX_IMPORT_DATE = date(2026, 8, 1)
ARCHIVE_DATE_TIME = datetime(2026, 9, 4, 13, 45, 10)

PARTITION_KEYS = [ArchiveKeys.archive_date, ArchiveKeys.run_number]


class TestMain:

    @patch(f"{PATCH_PATH}.save_run_log")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.aUtils.get_run_number")
    @patch(f"{PATCH_PATH}.datetime")
    def test_main_runs(
        self,
        datetime_mock: Mock,
        get_run_number_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        save_run_log_mock: Mock,
    ):
        datetime_mock.now.return_value = ARCHIVE_DATE_TIME
        get_run_number_mock.return_value = 2
        scan_parquet_mock.side_effect = [
            pl.LazyFrame({IndCQC.cqc_location_import_date: [MAX_IMPORT_DATE]}),
            pl.LazyFrame({"dummy": [2]}),
        ]

        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            RUN_LOG_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 2
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    ESTIMATES_SOURCE,
                    selected_columns=job.JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
                ),
                call(
                    METADATA_SOURCE,
                    selected_columns=job.JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
                ),
            ]
        )

        get_run_number_mock.assert_called_once_with(
            [ESTIMATES_DESTINATION, METADATA_DESTINATION]
        )

        assert sink_to_parquet_mock.call_count == 2
        expected_destinations = [
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
        ]
        for sink_call, expected_destination in zip(
            sink_to_parquet_mock.call_args_list, expected_destinations
        ):
            sunk_lf, destination = sink_call.args
            assert destination == expected_destination
            assert sink_call.kwargs["partition_cols"] == PARTITION_KEYS

            collected = sunk_lf.collect()
            assert collected[ArchiveKeys.archive_date].to_list() == ["2026-09-04"]
            assert collected[ArchiveKeys.run_number].to_list() == [3]

        save_run_log_mock.assert_called_once_with(
            "2026-09-04",
            3,
            MAX_IMPORT_DATE,
            ARCHIVE_DATE_TIME,
            RUN_LOG_DESTINATION,
        )


class TestSaveRunLog:

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_sinks_run_log_partitioned_by_archive_keys(
        self, sink_to_parquet_mock: Mock
    ):
        job.save_run_log(
            "2026-09-04",
            3,
            MAX_IMPORT_DATE,
            ARCHIVE_DATE_TIME,
            RUN_LOG_DESTINATION,
        )

        sink_to_parquet_mock.assert_called_once()
        assert sink_to_parquet_mock.call_args.args[1] == RUN_LOG_DESTINATION
        assert sink_to_parquet_mock.call_args.kwargs["partition_cols"] == PARTITION_KEYS

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    def test_sinks_expected_run_log(self, sink_to_parquet_mock: Mock):
        job.save_run_log(
            "2026-09-04",
            3,
            MAX_IMPORT_DATE,
            ARCHIVE_DATE_TIME,
            RUN_LOG_DESTINATION,
        )

        returned_lf = sink_to_parquet_mock.call_args.args[0]
        expected_lf = pl.LazyFrame(
            [
                (
                    "2026-09-04",
                    3,
                    ARCHIVE_DATE_TIME,
                    MAX_IMPORT_DATE,
                    False,
                    None,
                    None,
                    False,
                    False,
                    False,
                )
            ],
            schema={
                ArchiveKeys.archive_date: pl.String,
                ArchiveKeys.run_number: pl.Int64,
                RunLogCols.archive_date_time: pl.Datetime,
                RunLogCols.max_cqc_location_import_date: pl.Date,
                RunLogCols.approved: pl.Boolean,
                RunLogCols.commit_sha: pl.String,
                RunLogCols.tag: pl.String,
                RunLogCols.selected_for_publication: pl.Boolean,
                RunLogCols.reconciled: pl.Boolean,
                RunLogCols.locally_checked: pl.Boolean,
            },
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
