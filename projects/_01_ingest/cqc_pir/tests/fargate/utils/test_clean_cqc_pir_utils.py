import polars as pl
import polars.testing as pl_testing
import pytest

import projects._01_ingest.cqc_pir.fargate.utils.clean_cqc_pir_utils as job
from projects._01_ingest.cqc_pir.unittest_data.cqc_pir_test_file_data import (
    ADD_CARE_HOME_COLUMN_EXPECTED_DATA,
    ADD_CARE_HOME_COLUMN_INPUT_DATA,
    FILTER_LATEST_SUBMISSION_DATE_CASES,
    NULL_LARGE_SINGLE_SUBMISSION_LOCATIONS_CASES,
    FilterLatestSubmissionDateTestCase,
    NullLargeSingleSubmissionLocationsTestCase,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols


class TestAddCareHomeColumn:
    def test_maps_pir_type_to_expected_care_home_value(self):
        input_lf = pl.LazyFrame(ADD_CARE_HOME_COLUMN_INPUT_DATA)

        returned_lf = job.add_care_home_column(input_lf)

        expected_lf = pl.LazyFrame(ADD_CARE_HOME_COLUMN_EXPECTED_DATA)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestFilterLatestSubmissionDate:
    @pytest.mark.parametrize(
        "case", [c.as_pytest_param() for c in FILTER_LATEST_SUBMISSION_DATE_CASES]
    )
    def test_returns_single_row_per_latest_submission_date(
        self, case: FilterLatestSubmissionDateTestCase
    ):
        input_lf = pl.LazyFrame(case.data)

        returned_lf = job.filter_latest_submission_date(input_lf)

        expected_lf = pl.LazyFrame(case.expected_data)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestNullPeopleDirectlyEmployedOutliers:
    def test_adds_cleaned_column_and_preserves_row_count(self):
        input_lf = pl.LazyFrame(
            {
                PIRClean.location_id: ["1-0001", "1-0001"],
                PIRClean.cqc_pir_import_date: ["2024-01-01", "2025-01-01"],
                PIRCols.pir_people_directly_employed: [1, 10],
            }
        )

        returned_df = job.null_people_directly_employed_outliers(input_lf).collect()

        assert PIRClean.pir_people_directly_employed_cleaned in returned_df.columns
        assert returned_df.height == input_lf.collect().height


class TestNullLargeSingleSubmissionLocations:
    @pytest.mark.parametrize(
        "case",
        [c.as_pytest_param() for c in NULL_LARGE_SINGLE_SUBMISSION_LOCATIONS_CASES],
    )
    def test_nulls_headcount_only_for_large_single_submission_locations(
        self, case: NullLargeSingleSubmissionLocationsTestCase
    ):
        input_lf = pl.LazyFrame(case.data)

        returned_lf = job.null_large_single_submission_locations(input_lf)

        expected_lf = pl.LazyFrame(case.expected_data)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
