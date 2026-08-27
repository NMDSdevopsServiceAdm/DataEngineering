from dataclasses import dataclass

import pytest

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

# --- clean_capacity_tracker_utils ---

AGENCY_AND_NON_AGENCY_DIFFER_INPUT_DATA = {
    CTCH.nurses_employed: [1, 1, 1],
    CTCH.agency_nurses_employed: [1, 1, 2],
    CTCH.care_workers_employed: [2, 2, 2],
    CTCH.agency_care_workers_employed: [2, 2, 2],
    CTCH.non_care_workers_employed: [3, 3, 3],
    CTCH.agency_non_care_workers_employed: [3, 4, 3],
}
AGENCY_AND_NON_AGENCY_DIFFER_EXPECTED_DATA = {
    CTCH.nurses_employed: [1, 1],
    CTCH.agency_nurses_employed: [1, 2],
    CTCH.care_workers_employed: [2, 2],
    CTCH.agency_care_workers_employed: [2, 2],
    CTCH.non_care_workers_employed: [3, 3],
    CTCH.agency_non_care_workers_employed: [4, 3],
}


@dataclass
class BoundColumnsTestCase:
    id: str
    data: dict
    expected_data: dict
    lower_limit: int | None = None
    upper_limit: int | None = None

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


BOUND_COLUMNS_TEST_CASES = [
    BoundColumnsTestCase(
        id="nulls_values_below_lower_limit",
        data={"a": [0, 1, 2], "b": [1, 0, 1]},
        expected_data={"a": [None, 1, 2], "b": [1, None, 1]},
        lower_limit=1,
    ),
    BoundColumnsTestCase(
        id="nulls_values_above_upper_limit",
        data={"a": [1, 2, 3]},
        expected_data={"a": [1, 2, None]},
        upper_limit=2,
    ),
    BoundColumnsTestCase(
        id="nulls_values_outside_lower_and_upper_limit",
        data={"a": [0, 1, 2, 3]},
        expected_data={"a": [None, 1, 2, None]},
        lower_limit=1,
        upper_limit=2,
    ),
    BoundColumnsTestCase(
        id="returns_unchanged_column_when_no_limits_given",
        data={"a": [0, 1, None]},
        expected_data={"a": [0, 1, None]},
    ),
]


@dataclass
class AddTotalEmployedColumnsTestCase:
    id: str
    data: dict
    expected_totals: dict

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


ADD_TOTAL_EMPLOYED_COLUMNS_TEST_CASES = [
    AddTotalEmployedColumnsTestCase(
        id="sums_non_agency_agency_and_combined_totals",
        data={
            CTCH.nurses_employed: [1],
            CTCH.care_workers_employed: [2],
            CTCH.non_care_workers_employed: [3],
            CTCH.agency_nurses_employed: [4],
            CTCH.agency_care_workers_employed: [5],
            CTCH.agency_non_care_workers_employed: [6],
        },
        expected_totals={
            CTCHClean.non_agency_total_employed: [6],
            CTCHClean.agency_total_employed: [15],
            CTCHClean.ct_care_home_total_employed: [21],
        },
    ),
    AddTotalEmployedColumnsTestCase(
        id="propagates_null_when_an_input_column_is_null",
        data={
            CTCH.nurses_employed: [1, None],
            CTCH.care_workers_employed: [2, 2],
            CTCH.non_care_workers_employed: [3, 3],
            CTCH.agency_nurses_employed: [4, 4],
            CTCH.agency_care_workers_employed: [5, 5],
            CTCH.agency_non_care_workers_employed: [6, 6],
        },
        expected_totals={
            CTCHClean.non_agency_total_employed: [6, None],
            CTCHClean.agency_total_employed: [15, 15],
            CTCHClean.ct_care_home_total_employed: [21, None],
        },
    ),
]

# --- clean_capacity_tracker_care_home_data ---

CLEAN_CARE_HOME_MAIN_INPUT_DATA = {
    CTCH.cqc_id: ["1-001"],
    CTCH.nurses_employed: ["1"],
    CTCH.care_workers_employed: ["2"],
    CTCH.non_care_workers_employed: ["3"],
    CTCH.agency_nurses_employed: ["0"],
    CTCH.agency_care_workers_employed: ["0"],
    CTCH.agency_non_care_workers_employed: ["0"],
    Keys.import_date: ["20240101"],
}

# --- clean_capacity_tracker_non_res_data ---

CLEAN_NON_RES_MAIN_INPUT_DATA = {
    CTNR.cqc_id: ["1-001"],
    CTNR.cqc_care_workers_employed: ["5"],
    CTNR.service_user_count: ["10"],
    Keys.import_date: ["20240101"],
}
CLEAN_NON_RES_OUT_OF_RANGE_INPUT_DATA = {
    CTNR.cqc_id: ["1-001"],
    CTNR.cqc_care_workers_employed: ["0"],
    CTNR.service_user_count: ["3001"],
    Keys.import_date: ["20240101"],
}

# --- validate_clean_capacity_tracker_care_home_data ---

VALIDATE_CARE_HOME_RAW_DATA = {
    CTCH.nurses_employed: [1, 2],
    # row 0's three job-role pairs all match, so it's excluded from the expected
    # cleaned row count (mirrors agency_and_non_agency_values_differ_filter)
    CTCH.agency_nurses_employed: [1, 9],
    CTCH.care_workers_employed: [1, 1],
    CTCH.agency_care_workers_employed: [1, 1],
    CTCH.non_care_workers_employed: [1, 1],
    CTCH.agency_non_care_workers_employed: [1, 1],
}
VALIDATE_CARE_HOME_CLEANED_DATA = {
    CTCHClean.cqc_id: ["1-001"],
    CTCHClean.ct_care_home_import_date: ["20240101"],
    CTCHClean.nurses_employed: [2],
    CTCHClean.care_workers_employed: [1],
    CTCHClean.non_care_workers_employed: [1],
    CTCHClean.agency_nurses_employed: [9],
    CTCHClean.agency_care_workers_employed: [1],
    CTCHClean.agency_non_care_workers_employed: [1],
    CTCHClean.non_agency_total_employed: [4],
    CTCHClean.agency_total_employed: [11],
    CTCHClean.ct_care_home_total_employed: [15],
}

# --- validate_clean_capacity_tracker_non_res_data ---

VALIDATE_NON_RES_RAW_DATA = {CTNRClean.cqc_id: ["1-001", "1-002"]}
VALIDATE_NON_RES_CLEANED_DATA = {
    CTNRClean.cqc_id: ["1-001", "1-002"],
    CTNRClean.ct_non_res_import_date: ["20240101", "20240101"],
    CTNRClean.cqc_care_workers_employed: [5, 10],
    CTNRClean.service_user_count: [10, 20],
}

# --- ingest_capacity_tracker_data ---

SANITISE_COLUMN_NAMES_INPUT_SCHEMA = ["Some Col", "Another(One)", "unchanged", "CqcId"]
SANITISE_COLUMN_NAMES_EXPECTED_SCHEMA = [
    "some_col",
    "anotherone",
    "unchanged",
    "cqcid",
]
# capacity_tracker_columns names multi-word raw columns as one smooshed lowercase
# word (e.g. "cqccareworkersemployed"), not snake_case, so sanitising must not
# insert underscores at word boundaries within a single PascalCase header.
SANITISE_COLUMN_NAMES_DOES_NOT_SNAKE_CASE_INPUT_SCHEMA = ["CqcCareWorkersEmployed"]
SANITISE_COLUMN_NAMES_DOES_NOT_SNAKE_CASE_EXPECTED_SCHEMA = ["cqccareworkersemployed"]
