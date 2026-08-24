from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.capacity_tracker.fargate.clean_capacity_tracker_care_home_data as job
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
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
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                CTCH.cqc_id: ["1-001"],
                CTCH.nurses_employed: ["1"],
                CTCH.care_workers_employed: ["2"],
                CTCH.non_care_workers_employed: ["3"],
                CTCH.agency_nurses_employed: ["0"],
                CTCH.agency_care_workers_employed: ["0"],
                CTCH.agency_non_care_workers_employed: ["0"],
                "import_date": ["20240101"],
            }
        )

        job.main("source", "destination")

        sink_to_parquet_mock.assert_called_once()
        returned_lf = sink_to_parquet_mock.call_args[0][0]
        assert sink_to_parquet_mock.call_args[0][1] == "destination"
        assert CTCHClean.care_home in returned_lf.collect_schema().names()
        assert (
            CTCHClean.ct_care_home_total_employed
            in returned_lf.collect_schema().names()
        )


class TestAddTotalEmployedColumns:
    def test_adds_non_agency_agency_and_combined_totals(self):
        test_lf = pl.LazyFrame(
            {
                CTCH.nurses_employed: [1],
                CTCH.care_workers_employed: [2],
                CTCH.non_care_workers_employed: [3],
                CTCH.agency_nurses_employed: [4],
                CTCH.agency_care_workers_employed: [5],
                CTCH.agency_non_care_workers_employed: [6],
            }
        )
        expected_lf = test_lf.with_columns(
            pl.lit(6, dtype=pl.Int64).alias(CTCHClean.non_agency_total_employed),
            pl.lit(15, dtype=pl.Int64).alias(CTCHClean.agency_total_employed),
            pl.lit(21, dtype=pl.Int64).alias(CTCHClean.ct_care_home_total_employed),
        )

        returned_lf = job.add_total_employed_columns(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
