from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
    PrimaryServiceType,
    PublishedJobRoleLabels,
)


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
class ReshapeJobRoleColsToRowsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


# The reshape unconditionally references every one of the 15 published labels' 4
# metric columns (trusting that the raw ASC-WDS schema always declares them), so
# every case below carries the full set - `other`'s columns are all-null in the
# single-row case, to also exercise the dense-null-row behaviour.
_reshape_single_row_case = ReshapeJobRoleColsToRowsTestCase(
    id="reshapes_all_labels_and_keeps_dense_row_when_a_labels_metrics_are_all_null",
    input_data={
        AWPClean.establishment_id: ["1"],
        AWPClean.ascwds_workplace_import_date: [date(2024, 1, 1)],
        f"{PublishedJobRoleLabels.senior_management}_emp": [1],
        f"{PublishedJobRoleLabels.senior_management}_strt": [2],
        f"{PublishedJobRoleLabels.senior_management}_stop": [3],
        f"{PublishedJobRoleLabels.senior_management}_vacy": [4],
        f"{PublishedJobRoleLabels.registered_manager}_emp": [2],
        f"{PublishedJobRoleLabels.registered_manager}_strt": [3],
        f"{PublishedJobRoleLabels.registered_manager}_stop": [4],
        f"{PublishedJobRoleLabels.registered_manager}_vacy": [5],
        f"{PublishedJobRoleLabels.social_worker}_emp": [3],
        f"{PublishedJobRoleLabels.social_worker}_strt": [4],
        f"{PublishedJobRoleLabels.social_worker}_stop": [5],
        f"{PublishedJobRoleLabels.social_worker}_vacy": [6],
        f"{PublishedJobRoleLabels.senior_care_worker}_emp": [4],
        f"{PublishedJobRoleLabels.senior_care_worker}_strt": [5],
        f"{PublishedJobRoleLabels.senior_care_worker}_stop": [6],
        f"{PublishedJobRoleLabels.senior_care_worker}_vacy": [7],
        f"{PublishedJobRoleLabels.care_worker}_emp": [5],
        f"{PublishedJobRoleLabels.care_worker}_strt": [6],
        f"{PublishedJobRoleLabels.care_worker}_stop": [7],
        f"{PublishedJobRoleLabels.care_worker}_vacy": [8],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_emp": [6],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_strt": [7],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_stop": [8],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_vacy": [9],
        f"{PublishedJobRoleLabels.occupational_therapist}_emp": [7],
        f"{PublishedJobRoleLabels.occupational_therapist}_strt": [8],
        f"{PublishedJobRoleLabels.occupational_therapist}_stop": [9],
        f"{PublishedJobRoleLabels.occupational_therapist}_vacy": [10],
        f"{PublishedJobRoleLabels.registered_nurse}_emp": [8],
        f"{PublishedJobRoleLabels.registered_nurse}_strt": [9],
        f"{PublishedJobRoleLabels.registered_nurse}_stop": [10],
        f"{PublishedJobRoleLabels.registered_nurse}_vacy": [11],
        f"{PublishedJobRoleLabels.allied_health_professional}_emp": [9],
        f"{PublishedJobRoleLabels.allied_health_professional}_strt": [10],
        f"{PublishedJobRoleLabels.allied_health_professional}_stop": [11],
        f"{PublishedJobRoleLabels.allied_health_professional}_vacy": [12],
        f"{PublishedJobRoleLabels.deputy_manager}_emp": [10],
        f"{PublishedJobRoleLabels.deputy_manager}_strt": [11],
        f"{PublishedJobRoleLabels.deputy_manager}_stop": [12],
        f"{PublishedJobRoleLabels.deputy_manager}_vacy": [13],
        f"{PublishedJobRoleLabels.support_worker}_emp": [11],
        f"{PublishedJobRoleLabels.support_worker}_strt": [12],
        f"{PublishedJobRoleLabels.support_worker}_stop": [13],
        f"{PublishedJobRoleLabels.support_worker}_vacy": [14],
        f"{PublishedJobRoleLabels.other_managers}_emp": [12],
        f"{PublishedJobRoleLabels.other_managers}_strt": [13],
        f"{PublishedJobRoleLabels.other_managers}_stop": [14],
        f"{PublishedJobRoleLabels.other_managers}_vacy": [15],
        f"{PublishedJobRoleLabels.other_regulated_professions}_emp": [13],
        f"{PublishedJobRoleLabels.other_regulated_professions}_strt": [14],
        f"{PublishedJobRoleLabels.other_regulated_professions}_stop": [15],
        f"{PublishedJobRoleLabels.other_regulated_professions}_vacy": [16],
        f"{PublishedJobRoleLabels.other_direct_care}_emp": [14],
        f"{PublishedJobRoleLabels.other_direct_care}_strt": [15],
        f"{PublishedJobRoleLabels.other_direct_care}_stop": [16],
        f"{PublishedJobRoleLabels.other_direct_care}_vacy": [17],
        f"{PublishedJobRoleLabels.other}_emp": [None],
        f"{PublishedJobRoleLabels.other}_strt": [None],
        f"{PublishedJobRoleLabels.other}_stop": [None],
        f"{PublishedJobRoleLabels.other}_vacy": [None],
    },
    expected_data={
        AWPClean.establishment_id: ["1"] * 15,
        AWPClean.ascwds_workplace_import_date: [date(2024, 1, 1)] * 15,
        SLVCols.job_role_label: [
            PublishedJobRoleLabels.senior_management,
            PublishedJobRoleLabels.registered_manager,
            PublishedJobRoleLabels.social_worker,
            PublishedJobRoleLabels.senior_care_worker,
            PublishedJobRoleLabels.care_worker,
            PublishedJobRoleLabels.community_support_and_outreach,
            PublishedJobRoleLabels.occupational_therapist,
            PublishedJobRoleLabels.registered_nurse,
            PublishedJobRoleLabels.allied_health_professional,
            PublishedJobRoleLabels.deputy_manager,
            PublishedJobRoleLabels.support_worker,
            PublishedJobRoleLabels.other_managers,
            PublishedJobRoleLabels.other_regulated_professions,
            PublishedJobRoleLabels.other_direct_care,
            PublishedJobRoleLabels.other,
        ],
        SLVCols.employees: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, None],
        SLVCols.starters: [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, None],
        SLVCols.leavers: [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, None],
        SLVCols.vacancies: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, None],
    },
)

# Two source rows, each with its own distinct value per label (row A = the label's
# position 1-15, row B = 100 + that position), so a grain-pairing bug (a value
# leaking or shuffling onto the wrong establishment/date after the explode) would
# show up as a mismatched row rather than passing by coincidence.
_reshape_multi_row_case = ReshapeJobRoleColsToRowsTestCase(
    id="keeps_grain_columns_paired_with_their_own_row_across_multiple_source_rows",
    input_data={
        AWPClean.establishment_id: ["10", "20"],
        AWPClean.ascwds_workplace_import_date: [date(2024, 2, 1), date(2024, 3, 1)],
        f"{PublishedJobRoleLabels.senior_management}_emp": [1, 101],
        f"{PublishedJobRoleLabels.senior_management}_strt": [1, 101],
        f"{PublishedJobRoleLabels.senior_management}_stop": [1, 101],
        f"{PublishedJobRoleLabels.senior_management}_vacy": [1, 101],
        f"{PublishedJobRoleLabels.registered_manager}_emp": [2, 102],
        f"{PublishedJobRoleLabels.registered_manager}_strt": [2, 102],
        f"{PublishedJobRoleLabels.registered_manager}_stop": [2, 102],
        f"{PublishedJobRoleLabels.registered_manager}_vacy": [2, 102],
        f"{PublishedJobRoleLabels.social_worker}_emp": [3, 103],
        f"{PublishedJobRoleLabels.social_worker}_strt": [3, 103],
        f"{PublishedJobRoleLabels.social_worker}_stop": [3, 103],
        f"{PublishedJobRoleLabels.social_worker}_vacy": [3, 103],
        f"{PublishedJobRoleLabels.senior_care_worker}_emp": [4, 104],
        f"{PublishedJobRoleLabels.senior_care_worker}_strt": [4, 104],
        f"{PublishedJobRoleLabels.senior_care_worker}_stop": [4, 104],
        f"{PublishedJobRoleLabels.senior_care_worker}_vacy": [4, 104],
        f"{PublishedJobRoleLabels.care_worker}_emp": [5, 105],
        f"{PublishedJobRoleLabels.care_worker}_strt": [5, 105],
        f"{PublishedJobRoleLabels.care_worker}_stop": [5, 105],
        f"{PublishedJobRoleLabels.care_worker}_vacy": [5, 105],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_emp": [6, 106],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_strt": [6, 106],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_stop": [6, 106],
        f"{PublishedJobRoleLabels.community_support_and_outreach}_vacy": [6, 106],
        f"{PublishedJobRoleLabels.occupational_therapist}_emp": [7, 107],
        f"{PublishedJobRoleLabels.occupational_therapist}_strt": [7, 107],
        f"{PublishedJobRoleLabels.occupational_therapist}_stop": [7, 107],
        f"{PublishedJobRoleLabels.occupational_therapist}_vacy": [7, 107],
        f"{PublishedJobRoleLabels.registered_nurse}_emp": [8, 108],
        f"{PublishedJobRoleLabels.registered_nurse}_strt": [8, 108],
        f"{PublishedJobRoleLabels.registered_nurse}_stop": [8, 108],
        f"{PublishedJobRoleLabels.registered_nurse}_vacy": [8, 108],
        f"{PublishedJobRoleLabels.allied_health_professional}_emp": [9, 109],
        f"{PublishedJobRoleLabels.allied_health_professional}_strt": [9, 109],
        f"{PublishedJobRoleLabels.allied_health_professional}_stop": [9, 109],
        f"{PublishedJobRoleLabels.allied_health_professional}_vacy": [9, 109],
        f"{PublishedJobRoleLabels.deputy_manager}_emp": [10, 110],
        f"{PublishedJobRoleLabels.deputy_manager}_strt": [10, 110],
        f"{PublishedJobRoleLabels.deputy_manager}_stop": [10, 110],
        f"{PublishedJobRoleLabels.deputy_manager}_vacy": [10, 110],
        f"{PublishedJobRoleLabels.support_worker}_emp": [11, 111],
        f"{PublishedJobRoleLabels.support_worker}_strt": [11, 111],
        f"{PublishedJobRoleLabels.support_worker}_stop": [11, 111],
        f"{PublishedJobRoleLabels.support_worker}_vacy": [11, 111],
        f"{PublishedJobRoleLabels.other_managers}_emp": [12, 112],
        f"{PublishedJobRoleLabels.other_managers}_strt": [12, 112],
        f"{PublishedJobRoleLabels.other_managers}_stop": [12, 112],
        f"{PublishedJobRoleLabels.other_managers}_vacy": [12, 112],
        f"{PublishedJobRoleLabels.other_regulated_professions}_emp": [13, 113],
        f"{PublishedJobRoleLabels.other_regulated_professions}_strt": [13, 113],
        f"{PublishedJobRoleLabels.other_regulated_professions}_stop": [13, 113],
        f"{PublishedJobRoleLabels.other_regulated_professions}_vacy": [13, 113],
        f"{PublishedJobRoleLabels.other_direct_care}_emp": [14, 114],
        f"{PublishedJobRoleLabels.other_direct_care}_strt": [14, 114],
        f"{PublishedJobRoleLabels.other_direct_care}_stop": [14, 114],
        f"{PublishedJobRoleLabels.other_direct_care}_vacy": [14, 114],
        f"{PublishedJobRoleLabels.other}_emp": [15, 115],
        f"{PublishedJobRoleLabels.other}_strt": [15, 115],
        f"{PublishedJobRoleLabels.other}_stop": [15, 115],
        f"{PublishedJobRoleLabels.other}_vacy": [15, 115],
    },
    expected_data={
        AWPClean.establishment_id: ["10"] * 15 + ["20"] * 15,
        AWPClean.ascwds_workplace_import_date: [date(2024, 2, 1)] * 15
        + [date(2024, 3, 1)] * 15,
        SLVCols.job_role_label: [
            PublishedJobRoleLabels.senior_management,
            PublishedJobRoleLabels.registered_manager,
            PublishedJobRoleLabels.social_worker,
            PublishedJobRoleLabels.senior_care_worker,
            PublishedJobRoleLabels.care_worker,
            PublishedJobRoleLabels.community_support_and_outreach,
            PublishedJobRoleLabels.occupational_therapist,
            PublishedJobRoleLabels.registered_nurse,
            PublishedJobRoleLabels.allied_health_professional,
            PublishedJobRoleLabels.deputy_manager,
            PublishedJobRoleLabels.support_worker,
            PublishedJobRoleLabels.other_managers,
            PublishedJobRoleLabels.other_regulated_professions,
            PublishedJobRoleLabels.other_direct_care,
            PublishedJobRoleLabels.other,
        ]
        * 2,
        SLVCols.employees: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        + [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
        SLVCols.starters: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        + [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
        SLVCols.leavers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        + [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
        SLVCols.vacancies: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
        + [101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115],
    },
)


@dataclass
class TestPrepareUtilsData:
    reshape_job_role_cols_to_rows_test_cases = [
        _reshape_single_row_case,
        _reshape_multi_row_case,
    ]

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


@dataclass
class CollapseJobRoleEstimatesToPublishedLabelsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class JoinDatasetsTestCase:
    id: str
    job_role_estimates_data: dict[str, Any]
    cleaned_ascwds_workplace_data: dict[str, Any]
    expected_data: dict[str, Any]


METRIC = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated


@dataclass
class TestMergeUtilsData:
    collapse_job_role_estimates_to_published_labels_test_cases = [
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="role_shared_by_both_taxonomies_passes_through_unchanged",
            input_data={
                IndCQC.id_per_locationid_import_date: [1],
                IndCQC.location_id: ["loc1"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.main_job_role_clean_labelled: [
                    PublishedJobRoleLabels.registered_nurse
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.regulated_professions],
                METRIC: [10.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [1],
                SLVCols.job_role_label: [PublishedJobRoleLabels.registered_nurse],
                IndCQC.location_id: ["loc1"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.main_job_group_labelled: [JobGroupLabels.regulated_professions],
                METRIC: [10.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="unpublished_managers_role_buckets_into_other_managers",
            input_data={
                IndCQC.id_per_locationid_import_date: [2],
                IndCQC.location_id: ["loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only],
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.middle_management
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers],
                METRIC: [5.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [2],
                SLVCols.job_role_label: [PublishedJobRoleLabels.other_managers],
                IndCQC.location_id: ["loc2"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers],
                METRIC: [5.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="unpublished_direct_care_role_buckets_into_other_direct_care",
            input_data={
                IndCQC.id_per_locationid_import_date: [3],
                IndCQC.location_id: ["loc3"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [
                    PrimaryServiceType.care_home_with_nursing
                ],
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.employment_support
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.direct_care],
                METRIC: [7.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [3],
                SLVCols.job_role_label: [PublishedJobRoleLabels.other_direct_care],
                IndCQC.location_id: ["loc3"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [
                    PrimaryServiceType.care_home_with_nursing
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.direct_care],
                METRIC: [7.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="unpublished_regulated_professions_role_buckets_into_other_regulated_professions",
            input_data={
                IndCQC.id_per_locationid_import_date: [4],
                IndCQC.location_id: ["loc4"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.safeguarding_officer
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.regulated_professions],
                METRIC: [3.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [4],
                SLVCols.job_role_label: [
                    PublishedJobRoleLabels.other_regulated_professions
                ],
                IndCQC.location_id: ["loc4"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.main_job_group_labelled: [JobGroupLabels.regulated_professions],
                METRIC: [3.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="unpublished_other_group_role_buckets_into_other",
            input_data={
                IndCQC.id_per_locationid_import_date: [5],
                IndCQC.location_id: ["loc5"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only],
                IndCQC.main_job_role_clean_labelled: [MainJobRoleLabels.admin_staff],
                IndCQC.main_job_group_labelled: [JobGroupLabels.other],
                METRIC: [9.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [5],
                SLVCols.job_role_label: [PublishedJobRoleLabels.other],
                IndCQC.location_id: ["loc5"],
                IndCQC.cqc_location_import_date: [date(2024, 1, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only],
                IndCQC.main_job_group_labelled: [JobGroupLabels.other],
                METRIC: [9.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="sums_multiple_unpublished_roles_that_collapse_into_the_same_bucket",
            input_data={
                IndCQC.id_per_locationid_import_date: [6, 6],
                IndCQC.location_id: ["loc6"] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_with_nursing]
                * 2,
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.middle_management,
                    MainJobRoleLabels.first_line_manager,
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers] * 2,
                METRIC: [4.0, 6.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [6],
                SLVCols.job_role_label: [PublishedJobRoleLabels.other_managers],
                IndCQC.location_id: ["loc6"],
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)],
                IndCQC.primary_service_type: [
                    PrimaryServiceType.care_home_with_nursing
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers],
                METRIC: [10.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="ignores_null_values_when_summing_a_bucket",
            input_data={
                IndCQC.id_per_locationid_import_date: [7, 7],
                IndCQC.location_id: ["loc7"] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only] * 2,
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.middle_management,
                    MainJobRoleLabels.first_line_manager,
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers] * 2,
                METRIC: [4.0, None],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [7],
                SLVCols.job_role_label: [PublishedJobRoleLabels.other_managers],
                IndCQC.location_id: ["loc7"],
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers],
                METRIC: [4.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="returns_null_when_every_role_in_a_bucket_is_null",
            input_data={
                IndCQC.id_per_locationid_import_date: [8, 8],
                IndCQC.location_id: ["loc8"] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.middle_management,
                    MainJobRoleLabels.first_line_manager,
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers] * 2,
                METRIC: [None, None],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [8],
                SLVCols.job_role_label: [PublishedJobRoleLabels.other_managers],
                IndCQC.location_id: ["loc8"],
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers],
                METRIC: [None],
            },
        ),
    ]

    join_datasets_test_cases = [
        JoinDatasetsTestCase(
            id="brings_across_slv_metrics_on_matching_key",
            job_role_estimates_data={
                IndCQC.establishment_id: ["e1"],
                IndCQC.ascwds_workplace_import_date: [date(2024, 1, 1)],
                SLVCols.job_role_label: [PublishedJobRoleLabels.care_worker],
                IndCQC.location_id: ["loc1"],
            },
            cleaned_ascwds_workplace_data={
                AWPClean.establishment_id: ["e1"],
                AWPClean.ascwds_workplace_import_date: [date(2024, 1, 1)],
                SLVCols.job_role_label: [PublishedJobRoleLabels.care_worker],
                SLVCols.employees: [10],
                SLVCols.starters: [2],
                SLVCols.leavers: [1],
                SLVCols.vacancies: [3],
            },
            expected_data={
                IndCQC.establishment_id: ["e1"],
                IndCQC.ascwds_workplace_import_date: [date(2024, 1, 1)],
                SLVCols.job_role_label: [PublishedJobRoleLabels.care_worker],
                IndCQC.location_id: ["loc1"],
                SLVCols.employees: [10],
                SLVCols.starters: [2],
                SLVCols.leavers: [1],
                SLVCols.vacancies: [3],
            },
        ),
        JoinDatasetsTestCase(
            id="keeps_estimates_row_with_null_metrics_when_no_workplace_match",
            job_role_estimates_data={
                IndCQC.establishment_id: ["e2"],
                IndCQC.ascwds_workplace_import_date: [date(2024, 1, 1)],
                SLVCols.job_role_label: [PublishedJobRoleLabels.care_worker],
                IndCQC.location_id: ["loc2"],
            },
            cleaned_ascwds_workplace_data={
                AWPClean.establishment_id: ["e_other"],
                AWPClean.ascwds_workplace_import_date: [date(2024, 1, 1)],
                SLVCols.job_role_label: [PublishedJobRoleLabels.care_worker],
                SLVCols.employees: [10],
                SLVCols.starters: [2],
                SLVCols.leavers: [1],
                SLVCols.vacancies: [3],
            },
            expected_data={
                IndCQC.establishment_id: ["e2"],
                IndCQC.ascwds_workplace_import_date: [date(2024, 1, 1)],
                SLVCols.job_role_label: [PublishedJobRoleLabels.care_worker],
                IndCQC.location_id: ["loc2"],
                SLVCols.employees: [None],
                SLVCols.starters: [None],
                SLVCols.leavers: [None],
                SLVCols.vacancies: [None],
            },
        ),
    ]
