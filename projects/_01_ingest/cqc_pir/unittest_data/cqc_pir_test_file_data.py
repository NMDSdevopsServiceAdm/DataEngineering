from dataclasses import dataclass
from datetime import date

import pytest

from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols

# --- clean_cqc_pir_utils.add_care_home_column ---

ADD_CARE_HOME_COLUMN_INPUT_DATA = {
    PIRCols.location_id: ["loc 1", "loc 2", "loc 3", "loc 4"],
    PIRCols.pir_type: ["Residential", "Shared Lives", None, "Community"],
}
ADD_CARE_HOME_COLUMN_EXPECTED_DATA = {
    **ADD_CARE_HOME_COLUMN_INPUT_DATA,
    PIRClean.care_home: ["Y", None, None, "N"],
}


# --- clean_cqc_pir_utils.filter_latest_submission_date ---


@dataclass
class FilterLatestSubmissionDateTestCase:
    id: str
    data: dict
    expected_data: dict

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


FILTER_LATEST_SUBMISSION_DATE_CASES = [
    FilterLatestSubmissionDateTestCase(
        id="keeps_only_latest_submission_per_location_import_date_and_care_home",
        data={
            PIRClean.location_id: ["1-1199876096"] * 6,
            PIRClean.care_home: ["Y", "Y", "Y", "Y", "N", "Y"],
            PIRClean.cqc_pir_import_date: [
                date(2022, 2, 1),
                date(2022, 7, 1),
                date(2023, 6, 1),
                date(2023, 6, 1),
                date(2023, 6, 1),
                date(2023, 6, 1),
            ],
            PIRClean.pir_submission_date_as_date: [
                date(2021, 5, 7),
                date(2022, 5, 20),
                date(2023, 5, 12),
                date(2023, 5, 24),
                date(2023, 5, 24),
                date(2023, 5, 24),
            ],
        },
        expected_data={
            PIRClean.location_id: ["1-1199876096"] * 4,
            PIRClean.care_home: ["Y", "Y", "N", "Y"],
            PIRClean.cqc_pir_import_date: [
                date(2022, 2, 1),
                date(2022, 7, 1),
                date(2023, 6, 1),
                date(2023, 6, 1),
            ],
            PIRClean.pir_submission_date_as_date: [
                date(2021, 5, 7),
                date(2022, 5, 20),
                date(2023, 5, 24),
                date(2023, 5, 24),
            ],
        },
    ),
]


# --- clean_cqc_pir_utils.null_large_single_submission_locations ---


@dataclass
class NullLargeSingleSubmissionLocationsTestCase:
    id: str
    data: dict
    expected_data: dict

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


NULL_LARGE_SINGLE_SUBMISSION_LOCATIONS_CASES = [
    NullLargeSingleSubmissionLocationsTestCase(
        id="nulls_large_headcount_only_when_location_submitted_once",
        data={
            PIRClean.location_id: [
                "1-0001",
                "1-0001",
                "1-0002",
                "1-0002",
                "1-0003",
                "1-0003",
                "1-0004",
                "1-0004",
            ],
            PIRClean.cqc_pir_import_date: [
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
            ],
            PIRClean.pir_people_directly_employed_cleaned: [
                None,
                99,
                None,
                100,
                99,
                100,
                500,
                600,
            ],
        },
        expected_data={
            PIRClean.location_id: [
                "1-0001",
                "1-0001",
                "1-0002",
                "1-0002",
                "1-0003",
                "1-0003",
                "1-0004",
                "1-0004",
            ],
            PIRClean.cqc_pir_import_date: [
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
                date(2024, 1, 1),
                date(2025, 1, 1),
            ],
            PIRClean.pir_people_directly_employed_cleaned: [
                None,
                99,
                None,
                None,
                99,
                100,
                500,
                600,
            ],
        },
    ),
]
