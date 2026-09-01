from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.capacity_tracker.fargate.clean_capacity_tracker_care_home_data as job
from projects._01_ingest.capacity_tracker.unittest_data.capacity_tracker_test_file_data import (
    CLEAN_CARE_HOME_MAIN_INPUT_DATA,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)

PATCH_PATH = (
    "projects._01_ingest.capacity_tracker.fargate.clean_capacity_tracker_care_home_data"
)


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_cleans_and_sinks_care_home_data(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(CLEAN_CARE_HOME_MAIN_INPUT_DATA)

        job.main("source", "destination")

        sink_to_parquet_mock.assert_called_once()
        returned_lf = sink_to_parquet_mock.call_args[0][0]
        assert sink_to_parquet_mock.call_args[0][1] == "destination"
        assert CTCHClean.care_home in returned_lf.collect_schema().names()
        assert (
            CTCHClean.ct_care_home_total_employed
            in returned_lf.collect_schema().names()
        )
