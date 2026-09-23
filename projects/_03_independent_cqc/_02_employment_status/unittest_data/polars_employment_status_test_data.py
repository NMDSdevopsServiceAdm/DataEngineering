from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils as prepare_worker_job
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusImputeTempColumns as ImputeTempCols,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusMagicNumberRateColumns as EmpStatRates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EmploymentStatusID,
    EmploymentStatusLabels,
    JobGroupLabels,
    MainJobRoleID,
    MainJobRoleLabels,
    PrimaryServiceType,
    PublishedJobRoleLabels,
    Region,
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


PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage,
    EmpStatus.temporary_percentage,
    EmpStatus.bank_or_pool_percentage,
    EmpStatus.agency_percentage,
    EmpStatus.other_percentage,
]
IMPUTED_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_imputed,
    EmpStatus.temporary_percentage_imputed,
    EmpStatus.bank_or_pool_percentage_imputed,
    EmpStatus.agency_percentage_imputed,
    EmpStatus.other_percentage_imputed,
]
ROLLING_AVERAGE_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_rolling_avg,
    EmpStatus.temporary_percentage_rolling_avg,
    EmpStatus.bank_or_pool_percentage_rolling_avg,
    EmpStatus.agency_percentage_rolling_avg,
    EmpStatus.other_percentage_rolling_avg,
]
FIRST_KNOWN_VALUE_COLUMNS = [
    ImputeTempCols.first_known_value_prefix + col for col in PERCENTAGE_COLUMNS
]
LAST_KNOWN_VALUE_COLUMNS = [
    ImputeTempCols.last_known_value_prefix + col for col in PERCENTAGE_COLUMNS
]


def status_split(
    permanent: list[Optional[float]], columns: list[str]
) -> dict[str, list[Optional[float]]]:
    """
    Spread each permanent share into a split across the 5 statuses that sums to 1.

    The remainder goes to agency and the other statuses are 0, so a case only has to state one
    value per row. A null permanent share is null in every status, matching the cleaned data,
    where the 5 percentages are either all populated or all null.

    Args:
        permanent (list[Optional[float]]): the permanent share for each row
        columns (list[str]): the 5 column names to use, in permanent, temporary, bank or pool,
            agency, other order

    Returns:
        dict[str, list[Optional[float]]]: the 5 columns of the split
    """
    permanent_col, temporary_col, bank_or_pool_col, agency_col, other_col = columns
    zeros = [None if value is None else 0.0 for value in permanent]
    return {
        permanent_col: permanent,
        temporary_col: zeros,
        bank_or_pool_col: zeros,
        agency_col: [None if value is None else 1 - value for value in permanent],
        other_col: zeros,
    }


@dataclass
class ImputeUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


FIVE_MONTHS = [date(2024, month, 1) for month in range(1, 6)]
CARE_WORKER = PublishedJobRoleLabels.care_worker
REGISTERED_NURSE = PublishedJobRoleLabels.registered_nurse


def short_term_imputation_case(
    id: str,
    dates: list[date],
    permanent: list[Optional[float]],
    expected_permanent: list[Optional[float]],
) -> ImputeUtilsTestCase:
    """
    Build a single location and job role case, splitting both permanent lists with
    `status_split` so the case only states the permanent share.
    """
    keys = {
        IndCQC.location_id: ["loc1"] * len(dates),
        IndCQC.published_job_role_label: [CARE_WORKER] * len(dates),
        IndCQC.cqc_location_import_date: dates,
    }
    input_data = {**keys, **status_split(permanent, PERCENTAGE_COLUMNS)}
    return ImputeUtilsTestCase(
        id=id,
        input_data=input_data,
        expected_data={
            **input_data,
            **status_split(expected_permanent, IMPUTED_PERCENTAGE_COLUMNS),
        },
    )


NON_RES = PrimaryServiceType.non_residential
CARE_HOME = PrimaryServiceType.care_home_only
LONDON = Region.london
NORTH_EAST = Region.north_east


def rolling_average_case(
    id: str,
    rows: list[tuple[str, str, str, str, date, Optional[float]]],
    expected_permanent: list[Optional[float]],
    extra_columns: Optional[dict[str, list[Any]]] = None,
) -> ImputeUtilsTestCase:
    """
    Build a rolling average case from (location, service, region, job role, date, permanent
    imputed share) rows, splitting the imputed and expected rolling shares with `status_split`.
    """
    locations, services, regions, roles, dates, permanent = map(list, zip(*rows))
    input_data = {
        IndCQC.location_id: locations,
        IndCQC.primary_service_type: services,
        IndCQC.current_region: regions,
        IndCQC.published_job_role_label: roles,
        IndCQC.cqc_location_import_date: dates,
        **status_split(permanent, IMPUTED_PERCENTAGE_COLUMNS),
        **(extra_columns or {}),
    }
    return ImputeUtilsTestCase(
        id=id,
        input_data=input_data,
        expected_data={
            **input_data,
            **status_split(expected_permanent, ROLLING_AVERAGE_PERCENTAGE_COLUMNS),
        },
    )


# Cases use a 3mo rolling period.
ROLLING_AVERAGE_PERIOD = "3mo"

# Cases use a 1y extrapolation period and 2y interpolation cap.
SHORT_TERM_EXTRAPOLATION_PERIOD = "1y"
SHORT_TERM_INTERPOLATION_CAP_PERIOD = "2y"


@dataclass
class TestImputeUtilsData:
    add_fill_boundaries_test_cases = [
        ImputeUtilsTestCase(
            id="adds_first_and_last_known_dates_and_values_per_location_and_job_role",
            input_data={
                IndCQC.location_id: ["loc1"] * 5,
                IndCQC.published_job_role_label: [CARE_WORKER] * 5,
                IndCQC.cqc_location_import_date: FIVE_MONTHS,
                **status_split([None, 0.2, None, 0.6, None], PERCENTAGE_COLUMNS),
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 5,
                IndCQC.published_job_role_label: [CARE_WORKER] * 5,
                IndCQC.cqc_location_import_date: FIVE_MONTHS,
                ImputeTempCols.first_known_date: [date(2024, 2, 1)] * 5,
                ImputeTempCols.last_known_date: [date(2024, 4, 1)] * 5,
                **status_split([0.2] * 5, FIRST_KNOWN_VALUE_COLUMNS),
                **status_split([0.6] * 5, LAST_KNOWN_VALUE_COLUMNS),
            },
        ),
        ImputeUtilsTestCase(
            id="adds_previous_and_next_known_dates_for_each_row",
            input_data={
                IndCQC.location_id: ["loc1"] * 5,
                IndCQC.published_job_role_label: [CARE_WORKER] * 5,
                IndCQC.cqc_location_import_date: FIVE_MONTHS,
                **status_split([None, 0.2, None, 0.6, None], PERCENTAGE_COLUMNS),
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 5,
                IndCQC.published_job_role_label: [CARE_WORKER] * 5,
                IndCQC.cqc_location_import_date: FIVE_MONTHS,
                ImputeTempCols.previous_known_date: [
                    None,
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                    date(2024, 4, 1),
                    date(2024, 4, 1),
                ],
                ImputeTempCols.next_known_date: [
                    date(2024, 2, 1),
                    date(2024, 2, 1),
                    date(2024, 4, 1),
                    date(2024, 4, 1),
                    None,
                ],
            },
        ),
        ImputeUtilsTestCase(
            id="keeps_job_roles_at_same_location_separate",
            input_data={
                IndCQC.location_id: ["loc1"] * 4,
                IndCQC.published_job_role_label: [CARE_WORKER] * 2
                + [REGISTERED_NURSE] * 2,
                IndCQC.cqc_location_import_date: FIVE_MONTHS[:2] * 2,
                **status_split([0.2, None, None, 0.6], PERCENTAGE_COLUMNS),
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 4,
                IndCQC.published_job_role_label: [CARE_WORKER] * 2
                + [REGISTERED_NURSE] * 2,
                IndCQC.cqc_location_import_date: FIVE_MONTHS[:2] * 2,
                ImputeTempCols.first_known_date: [date(2024, 1, 1)] * 2
                + [date(2024, 2, 1)] * 2,
                ImputeTempCols.last_known_date: [date(2024, 1, 1)] * 2
                + [date(2024, 2, 1)] * 2,
                **status_split([0.2, 0.2, 0.6, 0.6], FIRST_KNOWN_VALUE_COLUMNS),
                **status_split([0.2, 0.2, 0.6, 0.6], LAST_KNOWN_VALUE_COLUMNS),
            },
        ),
        ImputeUtilsTestCase(
            id="returns_null_boundaries_when_group_has_no_known_values",
            input_data={
                IndCQC.location_id: ["loc1"] * 2,
                IndCQC.published_job_role_label: [CARE_WORKER] * 2,
                IndCQC.cqc_location_import_date: FIVE_MONTHS[:2],
                **status_split([None, None], PERCENTAGE_COLUMNS),
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 2,
                IndCQC.published_job_role_label: [CARE_WORKER] * 2,
                IndCQC.cqc_location_import_date: FIVE_MONTHS[:2],
                ImputeTempCols.first_known_date: [None] * 2,
                ImputeTempCols.last_known_date: [None] * 2,
                ImputeTempCols.previous_known_date: [None] * 2,
                ImputeTempCols.next_known_date: [None] * 2,
                **status_split([None, None], FIRST_KNOWN_VALUE_COLUMNS),
                **status_split([None, None], LAST_KNOWN_VALUE_COLUMNS),
            },
        ),
    ]

    add_short_term_imputed_percentages_test_cases = [
        short_term_imputation_case(
            id="does_not_change_known_values",
            dates=FIVE_MONTHS,
            permanent=[0.2, 0.3, 0.4, 0.5, 0.6],
            expected_permanent=[0.2, 0.3, 0.4, 0.5, 0.6],
        ),
        short_term_imputation_case(
            id="interpolates_gap_by_date_not_by_row",
            dates=[date(2024, 1, 1), date(2024, 1, 11), date(2024, 1, 31)],
            permanent=[0.2, None, 0.5],
            expected_permanent=[0.2, 0.3, 0.5],
        ),
        short_term_imputation_case(
            id="interpolates_gap_equal_to_cap",
            dates=[date(2021, 1, 1), date(2022, 1, 1), date(2023, 1, 1)],
            permanent=[0.2, None, 0.6],
            expected_permanent=[0.2, 0.4, 0.6],
        ),
        short_term_imputation_case(
            id="does_not_interpolate_gap_wider_than_cap",
            dates=[date(2021, 1, 1), date(2022, 1, 1), date(2023, 1, 2)],
            permanent=[0.2, None, 0.6],
            expected_permanent=[0.2, None, 0.6],
        ),
        short_term_imputation_case(
            id="carries_last_known_value_forwards_within_extrapolation_period",
            dates=[date(2021, 1, 1), date(2021, 6, 1), date(2022, 1, 1)],
            permanent=[0.3, None, None],
            expected_permanent=[0.3, 0.3, 0.3],
        ),
        short_term_imputation_case(
            id="does_not_carry_forwards_beyond_extrapolation_period",
            dates=[date(2021, 1, 1), date(2022, 2, 1)],
            permanent=[0.3, None],
            expected_permanent=[0.3, None],
        ),
        short_term_imputation_case(
            id="carries_first_known_value_backwards_within_extrapolation_period",
            dates=[date(2021, 1, 1), date(2021, 6, 1), date(2022, 1, 1)],
            permanent=[None, None, 0.3],
            expected_permanent=[0.3, 0.3, 0.3],
        ),
        short_term_imputation_case(
            id="does_not_carry_backwards_beyond_extrapolation_period",
            dates=[date(2020, 12, 1), date(2022, 1, 1)],
            permanent=[None, 0.3],
            expected_permanent=[None, 0.3],
        ),
        short_term_imputation_case(
            id="leaves_null_when_group_has_no_known_values",
            dates=FIVE_MONTHS[:2],
            permanent=[None, None],
            expected_permanent=[None, None],
        ),
        ImputeUtilsTestCase(
            id="imputes_all_five_percentage_columns_and_they_sum_to_one",
            input_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.published_job_role_label: [CARE_WORKER] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 11),
                    date(2024, 1, 31),
                ],
                EmpStatus.permanent_percentage: [0.5, None, 0.2],
                EmpStatus.temporary_percentage: [0.1, None, 0.4],
                EmpStatus.bank_or_pool_percentage: [0.1, None, 0.1],
                EmpStatus.agency_percentage: [0.2, None, 0.2],
                EmpStatus.other_percentage: [0.1, None, 0.1],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 3,
                IndCQC.published_job_role_label: [CARE_WORKER] * 3,
                IndCQC.cqc_location_import_date: [
                    date(2024, 1, 1),
                    date(2024, 1, 11),
                    date(2024, 1, 31),
                ],
                EmpStatus.permanent_percentage: [0.5, None, 0.2],
                EmpStatus.temporary_percentage: [0.1, None, 0.4],
                EmpStatus.bank_or_pool_percentage: [0.1, None, 0.1],
                EmpStatus.agency_percentage: [0.2, None, 0.2],
                EmpStatus.other_percentage: [0.1, None, 0.1],
                EmpStatus.permanent_percentage_imputed: [0.5, 0.4, 0.2],
                EmpStatus.temporary_percentage_imputed: [0.1, 0.2, 0.4],
                EmpStatus.bank_or_pool_percentage_imputed: [0.1, 0.1, 0.1],
                EmpStatus.agency_percentage_imputed: [0.2, 0.2, 0.2],
                EmpStatus.other_percentage_imputed: [0.1, 0.1, 0.1],
            },
        ),
        short_term_imputation_case(
            id="drops_temporary_columns",
            dates=FIVE_MONTHS[:1],
            permanent=[0.2],
            expected_permanent=[0.2],
        ),
    ]

    add_rolling_average_percentages_test_cases = [
        rolling_average_case(
            id="averages_across_locations_with_same_service_region_and_job_role",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc2", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.6),
            ],
            expected_permanent=[0.4, 0.4],
        ),
        rolling_average_case(
            id="weights_each_location_equally",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc2", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.6),
            ],
            expected_permanent=[0.4, 0.4],
            extra_columns={EmpStatus.employee_count: [10, 90]},
        ),
        rolling_average_case(
            id="only_includes_dates_within_rolling_window",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 2, 1), 0.4),
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 4, 1), 0.6),
            ],
            expected_permanent=[0.2, 0.3, 0.5],
        ),
        rolling_average_case(
            id="keeps_service_types_separate",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc2", CARE_HOME, LONDON, CARE_WORKER, date(2024, 1, 1), 0.6),
            ],
            expected_permanent=[0.2, 0.6],
        ),
        rolling_average_case(
            id="keeps_regions_separate",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc2", NON_RES, NORTH_EAST, CARE_WORKER, date(2024, 1, 1), 0.6),
            ],
            expected_permanent=[0.2, 0.6],
        ),
        rolling_average_case(
            id="keeps_job_roles_separate",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc1", NON_RES, LONDON, REGISTERED_NURSE, date(2024, 1, 1), 0.6),
            ],
            expected_permanent=[0.2, 0.6],
        ),
        rolling_average_case(
            id="ignores_null_imputed_values",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
                ("loc2", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), None),
            ],
            expected_permanent=[0.2, 0.2],
        ),
        rolling_average_case(
            id="carries_nearest_average_into_date_with_no_contributing_locations",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), None),
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 2, 1), 0.4),
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 9, 1), None),
            ],
            expected_permanent=[0.4, 0.4, 0.4],
        ),
        rolling_average_case(
            id="drops_temporary_columns",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, 1, 1), 0.2),
            ],
            expected_permanent=[0.2],
        ),
    ]
