from dataclasses import dataclass
from datetime import date
from typing import Any

import projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils as prepare_worker_job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusMagicNumberRateColumns as EmpStatRates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EmploymentStatusFilteringRule,
    EmploymentStatusID,
    EmploymentStatusLabels,
    JobGroupLabels,
    MainJobRoleID,
    MainJobRoleLabels,
    PrimaryServiceType,
    PublishedJobRoleLabels,
)


@dataclass
class CollapseJobRolesToPublishedLabelsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class AggregateEmploymentStatusDataTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class ReshapeEmploymentStatusDataTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


EMPLSTAT_PERM_COUNT = prepare_worker_job.EMPLOYMENT_STATUS_LABEL_TO_COLUMN[
    EmploymentStatusLabels.permanent
]
EMPLSTAT_TEMP_COUNT = prepare_worker_job.EMPLOYMENT_STATUS_LABEL_TO_COLUMN[
    EmploymentStatusLabels.temporary
]
EMPLSTAT_BANK_OR_POOL_COUNT = prepare_worker_job.EMPLOYMENT_STATUS_LABEL_TO_COLUMN[
    EmploymentStatusLabels.bank_or_pool
]
EMPLSTAT_AGENCY_COUNT = prepare_worker_job.EMPLOYMENT_STATUS_LABEL_TO_COLUMN[
    EmploymentStatusLabels.agency
]
EMPLSTAT_OTHER_COUNT = prepare_worker_job.EMPLOYMENT_STATUS_LABEL_TO_COLUMN[
    EmploymentStatusLabels.other
]


@dataclass
class TestPrepareWorkerUtilsData:
    collapse_job_roles_to_published_labels_test_cases = [
        CollapseJobRolesToPublishedLabelsTestCase(
            id="role_shared_by_both_taxonomies_passes_through_unchanged",
            input_data={
                AWKClean.main_job_role_clean_labelled: [MainJobRoleLabels.care_worker],
            },
            expected_data={
                AWKClean.main_job_role_clean_labelled: [MainJobRoleLabels.care_worker],
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker],
            },
        ),
        CollapseJobRolesToPublishedLabelsTestCase(
            id="unpublished_role_is_bucketed_into_its_job_groups_other_label",
            input_data={
                AWKClean.main_job_role_clean_labelled: [
                    MainJobRoleLabels.safeguarding_officer
                ],
            },
            expected_data={
                AWKClean.main_job_role_clean_labelled: [
                    MainJobRoleLabels.safeguarding_officer
                ],
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.other_regulated_professions
                ],
            },
        ),
    ]

    aggregate_employment_status_data_test_cases = [
        AggregateEmploymentStatusDataTestCase(
            id="aggregates_multiple_workers_and_drops_worker_level_columns",
            input_data={
                AWKClean.worker_id: ["1", "2"],
                AWKClean.location_id: ["loc1", "loc1"],
                AWKClean.establishment_id: ["1-001", "1-001"],
                AWKClean.ascwds_worker_import_date: [date(2024, 1, 1)] * 2,
                AWKClean.main_job_role_id: [MainJobRoleID.care_worker] * 2,
                AWKClean.main_job_role_clean: [MainJobRoleID.care_worker] * 2,
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker] * 2,
                AWKClean.employment_status: [EmploymentStatusID.permanent] * 2,
                AWKClean.employment_status_clean: [EmploymentStatusID.permanent] * 2,
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent
                ]
                * 2,
            },
            expected_data={
                AWKClean.location_id: ["loc1"],
                AWKClean.establishment_id: ["1-001"],
                AWKClean.ascwds_worker_import_date: [date(2024, 1, 1)],
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker],
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent
                ],
                EmpStatus.employment_status_count: [2],
            },
        ),
        AggregateEmploymentStatusDataTestCase(
            id="keeps_distinct_employment_status_groups_separate",
            input_data={
                AWKClean.worker_id: ["1", "2"],
                AWKClean.location_id: ["loc2", "loc2"],
                AWKClean.establishment_id: ["1-002", "1-002"],
                AWKClean.ascwds_worker_import_date: [date(2024, 2, 1)] * 2,
                AWKClean.main_job_role_clean: [MainJobRoleID.care_worker] * 2,
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker] * 2,
                AWKClean.employment_status_clean: [
                    EmploymentStatusID.permanent,
                    EmploymentStatusID.temporary,
                ],
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent,
                    EmploymentStatusLabels.temporary,
                ],
            },
            expected_data={
                AWKClean.location_id: ["loc2", "loc2"],
                AWKClean.establishment_id: ["1-002", "1-002"],
                AWKClean.ascwds_worker_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker] * 2,
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent,
                    EmploymentStatusLabels.temporary,
                ],
                EmpStatus.employment_status_count: [1, 1],
            },
        ),
        AggregateEmploymentStatusDataTestCase(
            id="keeps_distinct_establishments_at_the_same_location_separate",
            input_data={
                AWKClean.worker_id: ["1", "2"],
                AWKClean.location_id: ["loc3", "loc3"],
                AWKClean.establishment_id: ["1-003", "1-004"],
                AWKClean.ascwds_worker_import_date: [date(2024, 3, 1)] * 2,
                AWKClean.main_job_role_clean: [MainJobRoleID.care_worker] * 2,
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker] * 2,
                AWKClean.employment_status_clean: [EmploymentStatusID.permanent] * 2,
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent
                ]
                * 2,
            },
            expected_data={
                AWKClean.location_id: ["loc3", "loc3"],
                AWKClean.establishment_id: ["1-003", "1-004"],
                AWKClean.ascwds_worker_import_date: [date(2024, 3, 1)] * 2,
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker] * 2,
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent
                ]
                * 2,
                EmpStatus.employment_status_count: [1, 1],
            },
        ),
    ]

    reshape_employment_status_data_test_cases = [
        ReshapeEmploymentStatusDataTestCase(
            id="pivots_single_status_into_its_column_and_zeros_the_rest",
            input_data={
                AWKClean.location_id: ["loc1"],
                AWKClean.establishment_id: ["1-001"],
                AWKClean.ascwds_worker_import_date: [date(2024, 1, 1)],
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker],
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent
                ],
                EmpStatus.employment_status_count: [3],
            },
            expected_data={
                AWKClean.location_id: ["loc1"],
                AWKClean.establishment_id: ["1-001"],
                AWKClean.ascwds_worker_import_date: [date(2024, 1, 1)],
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker],
                EMPLSTAT_PERM_COUNT: [3],
                EMPLSTAT_TEMP_COUNT: [0],
                EMPLSTAT_BANK_OR_POOL_COUNT: [0],
                EMPLSTAT_AGENCY_COUNT: [0],
                EMPLSTAT_OTHER_COUNT: [0],
            },
        ),
        ReshapeEmploymentStatusDataTestCase(
            id="pivots_multiple_statuses_for_the_same_group_into_one_row",
            input_data={
                AWKClean.location_id: ["loc2", "loc2"],
                AWKClean.establishment_id: ["1-002", "1-002"],
                AWKClean.ascwds_worker_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker] * 2,
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent,
                    EmploymentStatusLabels.temporary,
                ],
                EmpStatus.employment_status_count: [2, 1],
            },
            expected_data={
                AWKClean.location_id: ["loc2"],
                AWKClean.establishment_id: ["1-002"],
                AWKClean.ascwds_worker_import_date: [date(2024, 2, 1)],
                IndCQC.published_job_role_label: [MainJobRoleLabels.care_worker],
                EMPLSTAT_PERM_COUNT: [2],
                EMPLSTAT_TEMP_COUNT: [1],
                EMPLSTAT_BANK_OR_POOL_COUNT: [0],
                EMPLSTAT_AGENCY_COUNT: [0],
                EMPLSTAT_OTHER_COUNT: [0],
            },
        ),
        ReshapeEmploymentStatusDataTestCase(
            id="keeps_distinct_groups_as_separate_rows",
            input_data={
                AWKClean.location_id: ["loc3", "loc3"],
                AWKClean.establishment_id: ["1-003", "1-003"],
                AWKClean.ascwds_worker_import_date: [date(2024, 3, 1)] * 2,
                IndCQC.published_job_role_label: [
                    MainJobRoleLabels.care_worker,
                    MainJobRoleLabels.registered_nurse,
                ],
                AWKClean.employment_status_clean_labelled: [
                    EmploymentStatusLabels.permanent,
                    EmploymentStatusLabels.agency,
                ],
                EmpStatus.employment_status_count: [1, 4],
            },
            expected_data={
                AWKClean.location_id: ["loc3", "loc3"],
                AWKClean.establishment_id: ["1-003", "1-003"],
                AWKClean.ascwds_worker_import_date: [date(2024, 3, 1)] * 2,
                IndCQC.published_job_role_label: [
                    MainJobRoleLabels.care_worker,
                    MainJobRoleLabels.registered_nurse,
                ],
                EMPLSTAT_PERM_COUNT: [1, 0],
                EMPLSTAT_TEMP_COUNT: [0, 0],
                EMPLSTAT_BANK_OR_POOL_COUNT: [0, 0],
                EMPLSTAT_AGENCY_COUNT: [0, 4],
                EMPLSTAT_OTHER_COUNT: [0, 0],
            },
        ),
    ]


@dataclass
class CollapseJobRoleEstimatesToPublishedLabelsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class ApplyEmploymentStatusMagicNumbersTestCase:
    id: str
    job_role_estimates_data: dict[str, Any]
    employment_status_rates_data: dict[str, Any]
    expected_data: dict[str, Any]


METRIC = IndCQC.estimate_filled_posts_by_job_role


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
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.registered_nurse
                ],
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
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.other_managers
                ],
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
                    MainJobRoleLabels.other_care_role
                ],
                IndCQC.main_job_group_labelled: [JobGroupLabels.direct_care],
                METRIC: [7.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [3],
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.other_direct_care
                ],
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
                IndCQC.published_job_role_label: [
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
                IndCQC.published_job_role_label: [PublishedJobRoleLabels.other],
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
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.other_managers
                ],
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
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.other_managers
                ],
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
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.other_managers
                ],
                IndCQC.location_id: ["loc8"],
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.main_job_group_labelled: [JobGroupLabels.managers],
                METRIC: [None],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="allocates_multiple_job_roles_into_different_published_job_roles",
            input_data={
                IndCQC.id_per_locationid_import_date: [9, 9],
                IndCQC.location_id: ["loc9"] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.care_worker,
                    MainJobRoleLabels.admin_staff,
                ],
                IndCQC.main_job_group_labelled: [
                    JobGroupLabels.direct_care,
                    JobGroupLabels.other,
                ],
                METRIC: [1.0, 2.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [9, 9],
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.care_worker,
                    PublishedJobRoleLabels.other,
                ],
                IndCQC.location_id: ["loc9"] * 2,
                IndCQC.cqc_location_import_date: [date(2024, 2, 1)] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.main_job_group_labelled: [
                    JobGroupLabels.direct_care,
                    JobGroupLabels.other,
                ],
                METRIC: [1.0, 2.0],
            },
        ),
        CollapseJobRoleEstimatesToPublishedLabelsTestCase(
            id="aggregates_id_per_location_import_date_separately",
            input_data={
                IndCQC.id_per_locationid_import_date: [10, 11],
                IndCQC.location_id: ["loc10", "loc11"],
                IndCQC.cqc_location_import_date: [
                    date(2024, 2, 1),
                    date(2024, 2, 2),
                ],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.main_job_role_clean_labelled: [
                    MainJobRoleLabels.care_worker,
                    MainJobRoleLabels.care_worker,
                ],
                IndCQC.main_job_group_labelled: [
                    JobGroupLabels.direct_care,
                    JobGroupLabels.direct_care,
                ],
                METRIC: [1.0, 2.0],
            },
            expected_data={
                IndCQC.id_per_locationid_import_date: [10, 11],
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.care_worker,
                    PublishedJobRoleLabels.care_worker,
                ],
                IndCQC.location_id: ["loc10", "loc11"],
                IndCQC.cqc_location_import_date: [
                    date(2024, 2, 1),
                    date(2024, 2, 2),
                ],
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 2,
                IndCQC.main_job_group_labelled: [
                    JobGroupLabels.direct_care,
                    JobGroupLabels.direct_care,
                ],
                METRIC: [1.0, 2.0],
            },
        ),
    ]


@dataclass
class TestMagicNumberUtilsData:
    apply_employment_status_magic_numbers_test_cases = [
        ApplyEmploymentStatusMagicNumbersTestCase(
            id="splits_filled_post_metric_by_employment_status_rates",
            job_role_estimates_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.published_job_role_label: [PublishedJobRoleLabels.care_worker],
                METRIC: [100.0],
                EmpStatus.employee_count: [50],
            },
            employment_status_rates_data={
                EmpStatRates.service: ["CQC Non residential"],
                EmpStatRates.weighting_job_role: ["Care_worker"],
                EmpStatRates.emp_stat_perm: [0.5],
                EmpStatRates.emp_stat_temp: [0.2],
                EmpStatRates.emp_stat_bank_or_pool: [0.15],
                EmpStatRates.emp_stat_agency: [0.1],
                EmpStatRates.emp_stat_other: [0.05],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.published_job_role_label: [PublishedJobRoleLabels.care_worker],
                METRIC: [100.0],
                EmpStatus.employee_count: [50],
                EmpStatus.estimated_emp_stat_perm: [50.0],
                EmpStatus.estimated_emp_stat_temp: [20.0],
                EmpStatus.estimated_emp_stat_bank_or_pool: [15.0],
                EmpStatus.estimated_emp_stat_agency: [10.0],
                EmpStatus.estimated_emp_stat_other: [5.0],
                EmpStatus.estimated_employees: [70.0],
            },
        ),
        ApplyEmploymentStatusMagicNumbersTestCase(
            id="maps_the_two_irregular_csv_labels_and_both_care_home_service_types",
            job_role_estimates_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.care_home_only,
                    PrimaryServiceType.care_home_with_nursing,
                ],
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.community_support_and_outreach,
                    PublishedJobRoleLabels.other,
                ],
                METRIC: [10.0, 8.0],
                EmpStatus.employee_count: [4, 10],
            },
            employment_status_rates_data={
                EmpStatRates.service: [
                    "CQC Care only home",
                    "CQC Care home with nursing",
                ],
                EmpStatRates.weighting_job_role: [
                    "Support_and_outreach",
                    "All_others",
                ],
                EmpStatRates.emp_stat_perm: [0.4, 0.5],
                EmpStatRates.emp_stat_temp: [0.1, 0.5],
                EmpStatRates.emp_stat_bank_or_pool: [0.2, 0.0],
                EmpStatRates.emp_stat_agency: [0.2, 0.0],
                EmpStatRates.emp_stat_other: [0.1, 0.0],
            },
            expected_data={
                IndCQC.primary_service_type: [
                    PrimaryServiceType.care_home_only,
                    PrimaryServiceType.care_home_with_nursing,
                ],
                IndCQC.published_job_role_label: [
                    PublishedJobRoleLabels.community_support_and_outreach,
                    PublishedJobRoleLabels.other,
                ],
                METRIC: [10.0, 8.0],
                EmpStatus.employee_count: [4, 10],
                EmpStatus.estimated_emp_stat_perm: [4.0, 4.0],
                EmpStatus.estimated_emp_stat_temp: [1.0, 4.0],
                EmpStatus.estimated_emp_stat_bank_or_pool: [2.0, 0.0],
                EmpStatus.estimated_emp_stat_agency: [2.0, 0.0],
                EmpStatus.estimated_emp_stat_other: [1.0, 0.0],
                EmpStatus.estimated_employees: [5.0, 8.0],
            },
        ),
        ApplyEmploymentStatusMagicNumbersTestCase(
            id="propagates_null_metric_to_all_split_columns_and_the_estimated_employees_column",
            job_role_estimates_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.published_job_role_label: [PublishedJobRoleLabels.care_worker],
                METRIC: [None],
                EmpStatus.employee_count: [5],
            },
            employment_status_rates_data={
                EmpStatRates.service: ["CQC Non residential"],
                EmpStatRates.weighting_job_role: ["Care_worker"],
                EmpStatRates.emp_stat_perm: [0.5],
                EmpStatRates.emp_stat_temp: [0.2],
                EmpStatRates.emp_stat_bank_or_pool: [0.15],
                EmpStatRates.emp_stat_agency: [0.1],
                EmpStatRates.emp_stat_other: [0.05],
            },
            expected_data={
                IndCQC.primary_service_type: [PrimaryServiceType.non_residential],
                IndCQC.published_job_role_label: [PublishedJobRoleLabels.care_worker],
                METRIC: [None],
                EmpStatus.employee_count: [5],
                EmpStatus.estimated_emp_stat_perm: [None],
                EmpStatus.estimated_emp_stat_temp: [None],
                EmpStatus.estimated_emp_stat_bank_or_pool: [None],
                EmpStatus.estimated_emp_stat_agency: [None],
                EmpStatus.estimated_emp_stat_other: [None],
                EmpStatus.estimated_employees: [None],
            },
        ),
    ]


@dataclass
class TestPrepareMainData:
    # Metadata is matched to a single workplace import date; the cleaned worker
    # source also carries an extra, unmatched date so filtering the worker's own
    # date column against metadata's workplace-keyed dates is exercised for real.
    metadata_matched_dates_data = {
        IndCQC.ascwds_workplace_import_date: [date(2024, 10, 8)],
    }
    cleaned_worker_with_extra_date_data = {
        AWKClean.location_id: ["loc1", "loc1"],
        AWKClean.establishment_id: ["1-001", "1-001"],
        AWKClean.ascwds_worker_import_date: [date(2024, 10, 1), date(2024, 10, 8)],
    }


CLEAN_UTILS_IMPORT_DATE = date(2024, 1, 1)


@dataclass
class CleanUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class TestCleanUtilsData:
    null_counts_for_low_location_ratio_test_cases = [
        CleanUtilsTestCase(
            id="sums_permanent_and_temporary_across_job_roles_before_comparing_to_location_staff",
            input_data={
                IndCQC.location_id: [
                    "loc_low_ratio",
                    "loc_low_ratio",
                    "loc_high_ratio",
                    "loc_high_ratio",
                    "loc_low_staff",
                ],
                IndCQC.establishment_id: [
                    "est_low_ratio",
                    "est_low_ratio",
                    "est_high_ratio",
                    "est_high_ratio",
                    "est_low_staff",
                ],
                IndCQC.published_job_role_label: [
                    "role_a",
                    "role_b",
                    "role_a",
                    "role_b",
                    "role_a",
                ],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 5,
                IndCQC.worker_records_bounded: [20, 20, 20, 20, 5],
                EmpStatus.permanent_count: [0, 0, 1, 0, 0],
                EmpStatus.temporary_count: [0, 0, 0, 0, 0],
                EmpStatus.bank_or_pool_count: [0, 0, 0, 0, 0],
                EmpStatus.agency_count: [0, 0, 0, 0, 0],
                EmpStatus.other_count: [0, 0, 0, 0, 0],
                EmpStatus.permanent_count_clean: [0, 0, 1, 0, 0],
                EmpStatus.temporary_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.agency_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.other_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.permanent_percentage: [0.5] * 5,
                EmpStatus.temporary_percentage: [0.5] * 5,
                EmpStatus.bank_or_pool_percentage: [0.5] * 5,
                EmpStatus.agency_percentage: [0.5] * 5,
                EmpStatus.other_percentage: [0.5] * 5,
                EmpStatus.permanent_percentage_clean: [0.5] * 5,
                EmpStatus.temporary_percentage_clean: [0.5] * 5,
                EmpStatus.bank_or_pool_percentage_clean: [0.5] * 5,
                EmpStatus.agency_percentage_clean: [0.5] * 5,
                EmpStatus.other_percentage_clean: [0.5] * 5,
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 5,
            },
            expected_data={
                IndCQC.location_id: [
                    "loc_low_ratio",
                    "loc_low_ratio",
                    "loc_high_ratio",
                    "loc_high_ratio",
                    "loc_low_staff",
                ],
                IndCQC.establishment_id: [
                    "est_low_ratio",
                    "est_low_ratio",
                    "est_high_ratio",
                    "est_high_ratio",
                    "est_low_staff",
                ],
                IndCQC.published_job_role_label: [
                    "role_a",
                    "role_b",
                    "role_a",
                    "role_b",
                    "role_a",
                ],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 5,
                IndCQC.worker_records_bounded: [20, 20, 20, 20, 5],
                EmpStatus.permanent_count: [0, 0, 1, 0, 0],
                EmpStatus.temporary_count: [0, 0, 0, 0, 0],
                EmpStatus.bank_or_pool_count: [0, 0, 0, 0, 0],
                EmpStatus.agency_count: [0, 0, 0, 0, 0],
                EmpStatus.other_count: [0, 0, 0, 0, 0],
                EmpStatus.permanent_count_clean: [None, None, 1, 0, 0],
                EmpStatus.temporary_count_clean: [None, None, 0, 0, 0],
                EmpStatus.bank_or_pool_count_clean: [None, None, 0, 0, 0],
                EmpStatus.agency_count_clean: [None, None, 0, 0, 0],
                EmpStatus.other_count_clean: [None, None, 0, 0, 0],
                # Raw percentages are never touched, unlike their _clean copies.
                EmpStatus.permanent_percentage: [0.5] * 5,
                EmpStatus.temporary_percentage: [0.5] * 5,
                EmpStatus.bank_or_pool_percentage: [0.5] * 5,
                EmpStatus.agency_percentage: [0.5] * 5,
                EmpStatus.other_percentage: [0.5] * 5,
                EmpStatus.permanent_percentage_clean: [None, None, 0.5, 0.5, 0.5],
                EmpStatus.temporary_percentage_clean: [None, None, 0.5, 0.5, 0.5],
                EmpStatus.bank_or_pool_percentage_clean: [None, None, 0.5, 0.5, 0.5],
                EmpStatus.agency_percentage_clean: [None, None, 0.5, 0.5, 0.5],
                EmpStatus.other_percentage_clean: [None, None, 0.5, 0.5, 0.5],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.populated,
                    EmploymentStatusFilteringRule.populated,
                    EmploymentStatusFilteringRule.populated,
                ],
            },
        ),
        CleanUtilsTestCase(
            id="keeps_org_level_reason_when_location_ratio_also_fails",
            input_data={
                IndCQC.location_id: ["loc_org_nulled"],
                IndCQC.establishment_id: ["est_org_nulled"],
                IndCQC.published_job_role_label: ["role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                IndCQC.worker_records_bounded: [20],
                EmpStatus.permanent_count: [0],
                EmpStatus.temporary_count: [0],
                EmpStatus.bank_or_pool_count: [0],
                EmpStatus.agency_count: [0],
                EmpStatus.other_count: [0],
                EmpStatus.permanent_count_clean: [None],
                EmpStatus.temporary_count_clean: [None],
                EmpStatus.bank_or_pool_count_clean: [None],
                EmpStatus.agency_count_clean: [None],
                EmpStatus.other_count_clean: [None],
                EmpStatus.permanent_percentage: [0.5],
                EmpStatus.temporary_percentage: [0.5],
                EmpStatus.bank_or_pool_percentage: [0.5],
                EmpStatus.agency_percentage: [0.5],
                EmpStatus.other_percentage: [0.5],
                EmpStatus.permanent_percentage_clean: [None],
                EmpStatus.temporary_percentage_clean: [None],
                EmpStatus.bank_or_pool_percentage_clean: [None],
                EmpStatus.agency_percentage_clean: [None],
                EmpStatus.other_percentage_clean: [None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ],
            },
            expected_data={
                IndCQC.location_id: ["loc_org_nulled"],
                IndCQC.establishment_id: ["est_org_nulled"],
                IndCQC.published_job_role_label: ["role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                IndCQC.worker_records_bounded: [20],
                EmpStatus.permanent_count: [0],
                EmpStatus.temporary_count: [0],
                EmpStatus.bank_or_pool_count: [0],
                EmpStatus.agency_count: [0],
                EmpStatus.other_count: [0],
                EmpStatus.permanent_count_clean: [None],
                EmpStatus.temporary_count_clean: [None],
                EmpStatus.bank_or_pool_count_clean: [None],
                EmpStatus.agency_count_clean: [None],
                EmpStatus.other_count_clean: [None],
                EmpStatus.permanent_percentage: [0.5],
                EmpStatus.temporary_percentage: [0.5],
                EmpStatus.bank_or_pool_percentage: [0.5],
                EmpStatus.agency_percentage: [0.5],
                EmpStatus.other_percentage: [0.5],
                EmpStatus.permanent_percentage_clean: [None],
                EmpStatus.temporary_percentage_clean: [None],
                EmpStatus.bank_or_pool_percentage_clean: [None],
                EmpStatus.agency_percentage_clean: [None],
                EmpStatus.other_percentage_clean: [None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ],
            },
        ),
    ]

    null_counts_for_low_org_ratio_test_cases = [
        CleanUtilsTestCase(
            id="nulls_org_at_boundary_ratio_and_staff_threshold",
            input_data={
                IndCQC.organisation_id: ["org1", "org1"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_b"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
            },
            expected_data={
                IndCQC.organisation_id: ["org1", "org1"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_b"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_count_clean: [None, None],
                EmpStatus.temporary_count_clean: [None, None],
                EmpStatus.bank_or_pool_count_clean: [None, None],
                EmpStatus.agency_count_clean: [None, None],
                EmpStatus.other_count_clean: [None, None],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
                EmpStatus.permanent_percentage_clean: [None, None],
                EmpStatus.temporary_percentage_clean: [None, None],
                EmpStatus.bank_or_pool_percentage_clean: [None, None],
                EmpStatus.agency_percentage_clean: [None, None],
                EmpStatus.other_percentage_clean: [None, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ]
                * 2,
            },
        ),
        CleanUtilsTestCase(
            id="does_not_inflate_org_staff_total_by_job_role_row_count",
            input_data={
                # 2 locations x 3 staff = 6 (below threshold); a per-row sum
                # instead of per-location dedup would double-count to 12.
                IndCQC.organisation_id: ["org2"] * 4,
                IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
                IndCQC.establishment_id: ["est1", "est1", "est2", "est2"],
                IndCQC.published_job_role_label: [
                    "role_a",
                    "role_b",
                    "role_a",
                    "role_b",
                ],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 4,
                IndCQC.worker_records_bounded: [3, 3, 3, 3],
                EmpStatus.permanent_count: [0, 0, 0, 0],
                EmpStatus.temporary_count: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count: [0, 0, 0, 0],
                EmpStatus.agency_count: [0, 0, 0, 0],
                EmpStatus.other_count: [0, 0, 0, 0],
                EmpStatus.permanent_percentage: [0.5] * 4,
                EmpStatus.temporary_percentage: [0.5] * 4,
                EmpStatus.bank_or_pool_percentage: [0.5] * 4,
                EmpStatus.agency_percentage: [0.5] * 4,
                EmpStatus.other_percentage: [0.5] * 4,
            },
            expected_data={
                IndCQC.organisation_id: ["org2"] * 4,
                IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
                IndCQC.establishment_id: ["est1", "est1", "est2", "est2"],
                IndCQC.published_job_role_label: [
                    "role_a",
                    "role_b",
                    "role_a",
                    "role_b",
                ],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 4,
                IndCQC.worker_records_bounded: [3, 3, 3, 3],
                EmpStatus.permanent_count: [0, 0, 0, 0],
                EmpStatus.temporary_count: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count: [0, 0, 0, 0],
                EmpStatus.agency_count: [0, 0, 0, 0],
                EmpStatus.other_count: [0, 0, 0, 0],
                EmpStatus.permanent_count_clean: [0, 0, 0, 0],
                EmpStatus.temporary_count_clean: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_clean: [0, 0, 0, 0],
                EmpStatus.agency_count_clean: [0, 0, 0, 0],
                EmpStatus.other_count_clean: [0, 0, 0, 0],
                EmpStatus.permanent_percentage: [0.5] * 4,
                EmpStatus.temporary_percentage: [0.5] * 4,
                EmpStatus.bank_or_pool_percentage: [0.5] * 4,
                EmpStatus.agency_percentage: [0.5] * 4,
                EmpStatus.other_percentage: [0.5] * 4,
                EmpStatus.permanent_percentage_clean: [0.5] * 4,
                EmpStatus.temporary_percentage_clean: [0.5] * 4,
                EmpStatus.bank_or_pool_percentage_clean: [0.5] * 4,
                EmpStatus.agency_percentage_clean: [0.5] * 4,
                EmpStatus.other_percentage_clean: [0.5] * 4,
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 4,
            },
        ),
        CleanUtilsTestCase(
            id="does_not_null_org_with_ratio_above_threshold",
            input_data={
                IndCQC.organisation_id: ["org3"],
                IndCQC.location_id: ["loc1"],
                IndCQC.establishment_id: ["est1"],
                IndCQC.published_job_role_label: ["role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                IndCQC.worker_records_bounded: [10],
                EmpStatus.permanent_count: [5],
                EmpStatus.temporary_count: [5],
                EmpStatus.bank_or_pool_count: [0],
                EmpStatus.agency_count: [0],
                EmpStatus.other_count: [0],
                EmpStatus.permanent_percentage: [0.5],
                EmpStatus.temporary_percentage: [0.5],
                EmpStatus.bank_or_pool_percentage: [0.5],
                EmpStatus.agency_percentage: [0.5],
                EmpStatus.other_percentage: [0.5],
            },
            expected_data={
                IndCQC.organisation_id: ["org3"],
                IndCQC.location_id: ["loc1"],
                IndCQC.establishment_id: ["est1"],
                IndCQC.published_job_role_label: ["role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                IndCQC.worker_records_bounded: [10],
                EmpStatus.permanent_count: [5],
                EmpStatus.temporary_count: [5],
                EmpStatus.bank_or_pool_count: [0],
                EmpStatus.agency_count: [0],
                EmpStatus.other_count: [0],
                EmpStatus.permanent_count_clean: [5],
                EmpStatus.temporary_count_clean: [5],
                EmpStatus.bank_or_pool_count_clean: [0],
                EmpStatus.agency_count_clean: [0],
                EmpStatus.other_count_clean: [0],
                EmpStatus.permanent_percentage: [0.5],
                EmpStatus.temporary_percentage: [0.5],
                EmpStatus.bank_or_pool_percentage: [0.5],
                EmpStatus.agency_percentage: [0.5],
                EmpStatus.other_percentage: [0.5],
                EmpStatus.permanent_percentage_clean: [0.5],
                EmpStatus.temporary_percentage_clean: [0.5],
                EmpStatus.bank_or_pool_percentage_clean: [0.5],
                EmpStatus.agency_percentage_clean: [0.5],
                EmpStatus.other_percentage_clean: [0.5],
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated],
            },
        ),
        CleanUtilsTestCase(
            id="does_not_null_rows_with_null_organisation_id_despite_low_ratio",
            input_data={
                # Without the null-org guard, .over() would pool these
                # unrelated locations into one fake org and could wrongly
                # trigger the rule for both.
                IndCQC.organisation_id: [None, None],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [0, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
            },
            expected_data={
                IndCQC.organisation_id: [None, None],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [0, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_count_clean: [0, 0],
                EmpStatus.temporary_count_clean: [0, 0],
                EmpStatus.bank_or_pool_count_clean: [0, 0],
                EmpStatus.agency_count_clean: [0, 0],
                EmpStatus.other_count_clean: [0, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
                EmpStatus.permanent_percentage_clean: [0.5, 0.5],
                EmpStatus.temporary_percentage_clean: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage_clean: [0.5, 0.5],
                EmpStatus.agency_percentage_clean: [0.5, 0.5],
                EmpStatus.other_percentage_clean: [0.5, 0.5],
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 2,
            },
        ),
        CleanUtilsTestCase(
            id="flags_missing_data_for_a_null_row_even_when_the_orgs_overall_ratio_is_fine",
            input_data={
                # loc1's 2nd job role has no worker records (null raw counts),
                # but its 1st job role alone keeps the org ratio fine - the
                # null row must not default to "populated".
                IndCQC.organisation_id: ["org4", "org4"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_b"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [10, None],
                EmpStatus.temporary_count: [0, None],
                EmpStatus.bank_or_pool_count: [0, None],
                EmpStatus.agency_count: [0, None],
                EmpStatus.other_count: [0, None],
                EmpStatus.permanent_percentage: [0.5, None],
                EmpStatus.temporary_percentage: [0.5, None],
                EmpStatus.bank_or_pool_percentage: [0.5, None],
                EmpStatus.agency_percentage: [0.5, None],
                EmpStatus.other_percentage: [0.5, None],
            },
            expected_data={
                IndCQC.organisation_id: ["org4", "org4"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_b"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [10, None],
                EmpStatus.temporary_count: [0, None],
                EmpStatus.bank_or_pool_count: [0, None],
                EmpStatus.agency_count: [0, None],
                EmpStatus.other_count: [0, None],
                EmpStatus.permanent_count_clean: [10, None],
                EmpStatus.temporary_count_clean: [0, None],
                EmpStatus.bank_or_pool_count_clean: [0, None],
                EmpStatus.agency_count_clean: [0, None],
                EmpStatus.other_count_clean: [0, None],
                EmpStatus.permanent_percentage: [0.5, None],
                EmpStatus.temporary_percentage: [0.5, None],
                EmpStatus.bank_or_pool_percentage: [0.5, None],
                EmpStatus.agency_percentage: [0.5, None],
                EmpStatus.other_percentage: [0.5, None],
                EmpStatus.permanent_percentage_clean: [0.5, None],
                EmpStatus.temporary_percentage_clean: [0.5, None],
                EmpStatus.bank_or_pool_percentage_clean: [0.5, None],
                EmpStatus.agency_percentage_clean: [0.5, None],
                EmpStatus.other_percentage_clean: [0.5, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.populated,
                    EmploymentStatusFilteringRule.missing_data,
                ],
            },
        ),
        CleanUtilsTestCase(
            id="nulls_both_locations_when_their_combined_org_ratio_is_too_low",
            input_data={
                # Neither location's own perm+temp/staff looks obviously bad in
                # isolation, but the combined org-wide ratio (1/20 = 0.05) is
                # exactly at threshold - both locations should be nulled.
                IndCQC.organisation_id: ["org5", "org5"],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [10, 10],
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
            },
            expected_data={
                IndCQC.organisation_id: ["org5", "org5"],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [10, 10],
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_count_clean: [None, None],
                EmpStatus.temporary_count_clean: [None, None],
                EmpStatus.bank_or_pool_count_clean: [None, None],
                EmpStatus.agency_count_clean: [None, None],
                EmpStatus.other_count_clean: [None, None],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
                EmpStatus.permanent_percentage_clean: [None, None],
                EmpStatus.temporary_percentage_clean: [None, None],
                EmpStatus.bank_or_pool_percentage_clean: [None, None],
                EmpStatus.agency_percentage_clean: [None, None],
                EmpStatus.other_percentage_clean: [None, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ]
                * 2,
            },
        ),
        CleanUtilsTestCase(
            id="does_not_pool_different_import_dates_into_one_org_group",
            input_data={
                # Same org and location, but two different import dates - a
                # low ratio on one date must not affect the other date's rows.
                IndCQC.organisation_id: ["org6", "org6"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [
                    CLEAN_UTILS_IMPORT_DATE,
                    date(2024, 2, 1),
                ],
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [0, 15],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
            },
            expected_data={
                IndCQC.organisation_id: ["org6", "org6"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [
                    CLEAN_UTILS_IMPORT_DATE,
                    date(2024, 2, 1),
                ],
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [0, 15],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_count_clean: [None, 15],
                EmpStatus.temporary_count_clean: [None, 0],
                EmpStatus.bank_or_pool_count_clean: [None, 0],
                EmpStatus.agency_count_clean: [None, 0],
                EmpStatus.other_count_clean: [None, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
                EmpStatus.permanent_percentage_clean: [None, 0.5],
                EmpStatus.temporary_percentage_clean: [None, 0.5],
                EmpStatus.bank_or_pool_percentage_clean: [None, 0.5],
                EmpStatus.agency_percentage_clean: [None, 0.5],
                EmpStatus.other_percentage_clean: [None, 0.5],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.populated,
                ],
            },
        ),
        CleanUtilsTestCase(
            id="does_not_inflate_org_permanent_temporary_total_by_repeated_job_role_row",
            input_data={
                # Same location+job role appearing twice (as if 2 CQC snapshots
                # mapped to the same ASCWDS submission). True org picture:
                # staff=20, permanent+temporary=1, ratio=0.05 (should trigger).
                # Summing both repeats instead of deduplicating would give
                # 2/20=0.1 and wrongly miss this.
                IndCQC.organisation_id: ["org7", "org7"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [1, 1],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
            },
            expected_data={
                IndCQC.organisation_id: ["org7", "org7"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.published_job_role_label: ["role_a", "role_a"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                IndCQC.worker_records_bounded: [20, 20],
                EmpStatus.permanent_count: [1, 1],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [0, 0],
                EmpStatus.permanent_count_clean: [None, None],
                EmpStatus.temporary_count_clean: [None, None],
                EmpStatus.bank_or_pool_count_clean: [None, None],
                EmpStatus.agency_count_clean: [None, None],
                EmpStatus.other_count_clean: [None, None],
                EmpStatus.permanent_percentage: [0.5, 0.5],
                EmpStatus.temporary_percentage: [0.5, 0.5],
                EmpStatus.bank_or_pool_percentage: [0.5, 0.5],
                EmpStatus.agency_percentage: [0.5, 0.5],
                EmpStatus.other_percentage: [0.5, 0.5],
                EmpStatus.permanent_percentage_clean: [None, None],
                EmpStatus.temporary_percentage_clean: [None, None],
                EmpStatus.bank_or_pool_percentage_clean: [None, None],
                EmpStatus.agency_percentage_clean: [None, None],
                EmpStatus.other_percentage_clean: [None, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ]
                * 2,
            },
        ),
    ]
