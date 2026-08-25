import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.capacity_tracker.fargate.utils.clean_capacity_tracker_utils as job
from projects._01_ingest.capacity_tracker.unittest_data.capacity_tracker_test_file_data import (
    ADD_TOTAL_EMPLOYED_COLUMNS_TEST_CASES,
    AGENCY_AND_NON_AGENCY_DIFFER_EXPECTED_DATA,
    AGENCY_AND_NON_AGENCY_DIFFER_INPUT_DATA,
    BOUND_COLUMNS_TEST_CASES,
)


class TestAgencyAndNonAgencyValuesDifferFilter:
    def test_removes_rows_where_all_three_pairs_match(self):
        test_lf = pl.LazyFrame(AGENCY_AND_NON_AGENCY_DIFFER_INPUT_DATA)
        expected_lf = pl.LazyFrame(AGENCY_AND_NON_AGENCY_DIFFER_EXPECTED_DATA)

        returned_lf = test_lf.filter(job.agency_and_non_agency_values_differ_filter())

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestBoundColumns:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in BOUND_COLUMNS_TEST_CASES]
    )
    def test_function_returns_expected_values(self, case):
        test_lf = pl.LazyFrame(case.data)
        expected_lf = pl.LazyFrame(case.expected_data)

        returned_lf = test_lf.with_columns(
            job.bound_columns(
                list(case.data.keys()),
                lower_limit=case.lower_limit,
                upper_limit=case.upper_limit,
            )
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddTotalEmployedColumns:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in ADD_TOTAL_EMPLOYED_COLUMNS_TEST_CASES]
    )
    def test_function_returns_expected_values(self, case):
        test_lf = pl.LazyFrame(case.data)
        expected_lf = test_lf.with_columns(
            [pl.Series(name, values) for name, values in case.expected_totals.items()]
        )

        returned_lf = job.add_total_employed_columns(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
