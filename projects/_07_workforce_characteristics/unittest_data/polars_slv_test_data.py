from dataclasses import dataclass
from typing import Any

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_values.categorical_column_values import PublishedJobRoleLabels


@dataclass
class ReduceToPublishedRolesTestCase:
    id: str
    mapping: dict[str, list[str]]
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
            id="merges_one_role_to_one_role",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
            },
            expected_data={
                AWPClean.job_role_01_employees: 3,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="merges_many_roles_to_one_role",
            mapping={"01": ["02", "03"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
                AWPClean.job_role_03_employees: 3,
            },
            expected_data={
                AWPClean.job_role_01_employees: 6,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="ignores_null_values_in_sum",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: None,
            },
            expected_data={
                AWPClean.job_role_01_employees: 1,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="returns_null_when_all_roles_are_null",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: None,
                AWPClean.job_role_02_employees: None,
            },
            expected_data={
                AWPClean.job_role_01_employees: None,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="ignores_roles_in_mapping_but_not_in_input_data",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
            },
            expected_data={
                AWPClean.job_role_01_employees: 1,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="creates_target_role_when_missing",
            mapping={"99": ["01", "02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
            },
            expected_data={
                "jr99emp": 3,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="merges_all_matching_suffixes",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
                AWPClean.job_role_01_starters: 3,
                AWPClean.job_role_02_starters: 4,
            },
            expected_data={
                AWPClean.job_role_01_employees: 3,
                AWPClean.job_role_01_starters: 7,
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="handles_extra_columns_in_input_data",
            mapping={"01": ["02"]},
            input_data={
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_02_employees: 2,
                "not_a_job_role_column": "A",
            },
            expected_data={
                AWPClean.job_role_01_employees: 3,
                "not_a_job_role_column": "A",
            },
        ),
        ReduceToPublishedRolesTestCase(
            id="handles_job_roles_with_same_characters",
            mapping={"101": ["10"]},
            input_data={
                AWPClean.job_role_10_employees: 10,
            },
            expected_data={
                "jr101emp": 10,
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
