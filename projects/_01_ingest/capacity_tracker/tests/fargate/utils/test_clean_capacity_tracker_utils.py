import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.capacity_tracker.fargate.utils.clean_capacity_tracker_utils as job
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)


class TestAgencyAndNonAgencyValuesDifferFilter:
    def test_removes_rows_where_all_three_pairs_match(self):
        test_lf = pl.LazyFrame(
            {
                CTCH.nurses_employed: [1, 1, 1],
                CTCH.agency_nurses_employed: [1, 1, 2],
                CTCH.care_workers_employed: [2, 2, 2],
                CTCH.agency_care_workers_employed: [2, 2, 2],
                CTCH.non_care_workers_employed: [3, 3, 3],
                CTCH.agency_non_care_workers_employed: [3, 4, 3],
            }
        )
        expected_lf = pl.LazyFrame(
            {
                CTCH.nurses_employed: [1, 1],
                CTCH.agency_nurses_employed: [1, 2],
                CTCH.care_workers_employed: [2, 2],
                CTCH.agency_care_workers_employed: [2, 2],
                CTCH.non_care_workers_employed: [3, 3],
                CTCH.agency_non_care_workers_employed: [4, 3],
            }
        )

        returned_lf = test_lf.filter(job.agency_and_non_agency_values_differ_filter())

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestBoundColumns:
    def test_nulls_values_below_lower_limit(self):
        test_lf = pl.LazyFrame({"a": [0, 1, 2], "b": [1, 0, 1]})
        expected_lf = pl.LazyFrame({"a": [None, 1, 2], "b": [1, None, 1]})

        returned_lf = test_lf.with_columns(job.bound_columns(["a", "b"], lower_limit=1))

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_nulls_values_above_upper_limit(self):
        test_lf = pl.LazyFrame({"a": [1, 2, 3]})
        expected_lf = pl.LazyFrame({"a": [1, 2, None]})

        returned_lf = test_lf.with_columns(job.bound_columns(["a"], upper_limit=2))

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_nulls_values_outside_lower_and_upper_limit(self):
        test_lf = pl.LazyFrame({"a": [0, 1, 2, 3]})
        expected_lf = pl.LazyFrame({"a": [None, 1, 2, None]})

        returned_lf = test_lf.with_columns(
            job.bound_columns(["a"], lower_limit=1, upper_limit=2)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_unchanged_column_when_no_limits_given(self):
        test_lf = pl.LazyFrame({"a": [0, 1, None]})

        returned_lf = test_lf.with_columns(job.bound_columns(["a"]))

        pl_testing.assert_frame_equal(returned_lf, test_lf)
