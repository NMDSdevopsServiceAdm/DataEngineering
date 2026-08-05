from dataclasses import dataclass
from typing import Any

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


@dataclass
class ReduceToPublishedRolesTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class TestPrepareUtilsData:
    reduce_to_published_roles_test_cases = [
        ReduceToPublishedRolesTestCase(
            id="leaves_published_role_untouched",
            input_data={
                AWPClean.job_role_01_employees: 1,  # senior_management - published
            },
            expected_data={
                AWPClean.job_role_01_employees: 1,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="folds_single_unpublished_role_into_its_job_group",
            input_data={
                AWPClean.job_role_35_employees: 5,  # safeguarding_officer -> regulated_professions
            },
            expected_data={
                "jr1002emp": 5,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="sums_multiple_unpublished_roles_into_same_job_group",
            input_data={
                AWPClean.job_role_02_employees: 2,  # middle_management -> managers
                AWPClean.job_role_03_employees: 3,  # first_line_manager -> managers
            },
            expected_data={
                "jr1001emp": 5,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="ignores_null_values_in_sum",
            input_data={
                AWPClean.job_role_02_employees: 2,
                AWPClean.job_role_03_employees: None,
            },
            expected_data={
                "jr1001emp": 2,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="returns_null_when_all_unpublished_roles_are_null",
            input_data={
                AWPClean.job_role_02_employees: None,
                AWPClean.job_role_03_employees: None,
            },
            expected_data={
                "jr1001emp": None,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="merges_all_matching_suffixes",
            input_data={
                AWPClean.job_role_02_employees: 2,
                AWPClean.job_role_03_employees: 3,
                AWPClean.job_role_02_starters: 4,
                AWPClean.job_role_03_starters: 5,
            },
            expected_data={
                "jr1001emp": 5,
                "jr1001strt": 9,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="handles_extra_non_job_role_columns_in_input_data",
            input_data={
                AWPClean.job_role_01_employees: 1,
                "not_a_job_role_column": "A",
            },
            expected_data={
                AWPClean.job_role_01_employees: 1,
                "not_a_job_role_column": "A",
            },
        ),
    ]
