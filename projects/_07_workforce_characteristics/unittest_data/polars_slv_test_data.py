from dataclasses import dataclass
from typing import Any

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_values.categorical_column_values import PublishedJobRoleLabels


@dataclass
class ReduceToPublishedRolesTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class RelabelJobRoleColumnsTestCase:
    id: str
    input_columns: list[str]
    expected_columns: list[str]


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
            id="folds_direct_care_group_role_into_its_job_group",
            input_data={
                AWPClean.job_role_10_employees: 7,  # employment_support -> direct_care
            },
            expected_data={
                "jr1003emp": 7,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="folds_other_group_role_into_its_job_group",
            input_data={
                AWPClean.job_role_25_employees: 4,  # admin_staff -> other
            },
            expected_data={
                "jr1004emp": 4,
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

    relabel_job_role_columns_test_cases = [
        RelabelJobRoleColumnsTestCase(
            id="known_code_renames_to_published_label_and_suffix",
            input_columns=[AWPClean.job_role_01_employees],
            expected_columns=[f"{PublishedJobRoleLabels.senior_management}_emp"],
        ),
        RelabelJobRoleColumnsTestCase(
            id="suffix_is_derived_per_column_not_a_fixed_list",
            input_columns=[
                AWPClean.job_role_04_starters,
                AWPClean.job_role_06_leavers,
                AWPClean.job_role_07_vacancies,
            ],
            expected_columns=[
                f"{PublishedJobRoleLabels.registered_manager}_strt",
                f"{PublishedJobRoleLabels.social_worker}_stop",
                f"{PublishedJobRoleLabels.senior_care_worker}_vacy",
            ],
        ),
        RelabelJobRoleColumnsTestCase(
            id="synthetic_merged_codes_rename_via_synthetic_dict",
            input_columns=["jr1001emp", "jr1002strt", "jr1003stop", "jr1004vacy"],
            expected_columns=[
                f"{PublishedJobRoleLabels.other_managers}_emp",
                f"{PublishedJobRoleLabels.other_regulated_professions}_strt",
                f"{PublishedJobRoleLabels.other_direct_care}_stop",
                f"{PublishedJobRoleLabels.other}_vacy",
            ],
        ),
        RelabelJobRoleColumnsTestCase(
            id="non_jr_prefixed_columns_are_left_untouched",
            input_columns=[AWPClean.establishment_id, AWPClean.job_role_08_employees],
            expected_columns=[
                AWPClean.establishment_id,
                f"{PublishedJobRoleLabels.care_worker}_emp",
            ],
        ),
        RelabelJobRoleColumnsTestCase(
            id="no_job_role_columns_present_is_a_no_op",
            input_columns=[
                AWPClean.establishment_id,
                AWPClean.ascwds_workplace_import_date,
            ],
            expected_columns=[
                AWPClean.establishment_id,
                AWPClean.ascwds_workplace_import_date,
            ],
        ),
    ]
