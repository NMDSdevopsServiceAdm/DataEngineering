from datetime import datetime
from unittest.mock import Mock, call, patch

import polars as pl

import projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)

PATCH_PATH = "projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates"

ESTIMATES_SOURCE = "some/estimates/directory"
METADATA_SOURCE = "some/metadata/directory"
ESTIMATES_DESTINATION = "some/estimates/destination"
METADATA_DESTINATION = "some/metadata/destination"
GEOGRAPHY_DESTINATION = "some/geography/destination"

PARTITION_KEYS = [ArchiveKeys.archive_date, ArchiveKeys.run_number]


class TestMain:
    @patch(f"{PATCH_PATH}.datetime")
    @patch(f"{PATCH_PATH}.aUtils.get_run_number")
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_and_sinks(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        get_run_number_mock: Mock,
        datetime_mock: Mock,
    ):
        datetime_mock.now.return_value = datetime(2026, 9, 4)
        get_run_number_mock.return_value = 2
        scan_parquet_mock.side_effect = [
            pl.LazyFrame({"dummy": [1]}),
            pl.LazyFrame({"dummy": [2]}),
            pl.LazyFrame({"dummy": [3]}),
        ]

        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 3
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
                call(
                    METADATA_SOURCE,
                    selected_columns=job.JOB_ROLE_GEOGRAPHY_ARCHIVE_COLUMNS,
                ),
            ]
        )

        get_run_number_mock.assert_called_once_with(
            [ESTIMATES_DESTINATION, METADATA_DESTINATION, GEOGRAPHY_DESTINATION]
        )

        assert sink_to_parquet_mock.call_count == 3
        expected_destinations = [
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
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
