from unittest.mock import Mock, patch

import polars as pl

import projects._01_ingest.capacity_tracker.fargate.clean_capacity_tracker_non_res_data as job
from projects._01_ingest.capacity_tracker.unittest_data.capacity_tracker_test_file_data import (
    CLEAN_NON_RES_MAIN_INPUT_DATA,
    CLEAN_NON_RES_OUT_OF_RANGE_INPUT_DATA,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)

PATCH_PATH = (
    "projects._01_ingest.capacity_tracker.fargate.clean_capacity_tracker_non_res_data"
)


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_cleans_and_sinks_non_res_data(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(CLEAN_NON_RES_MAIN_INPUT_DATA)

        job.main("source", "destination")

        sink_to_parquet_mock.assert_called_once()
        returned_lf = sink_to_parquet_mock.call_args[0][0]
        assert sink_to_parquet_mock.call_args[0][1] == "destination"

        returned_df = returned_lf.collect()
        assert returned_df[CTNRClean.care_home][0] == "N"
        assert returned_df[CTNR.cqc_care_workers_employed][0] == 5
        assert returned_df[CTNR.service_user_count][0] == 10

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_bounds_values_outside_valid_range(
        self, scan_parquet_mock: Mock, sink_to_parquet_mock: Mock
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(
            CLEAN_NON_RES_OUT_OF_RANGE_INPUT_DATA
        )

        job.main("source", "destination")

        returned_df = sink_to_parquet_mock.call_args[0][0].collect()
        assert returned_df[CTNR.cqc_care_workers_employed][0] is None
        assert returned_df[CTNR.service_user_count][0] is None
