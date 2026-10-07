from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional

import projects._03_independent_cqc._02_employment_status.fargate.utils.prepare_worker_utils as prepare_worker_job
from projects._03_independent_cqc._02_employment_status.fargate.utils.estimate_utils import (
    ESTIMATED_COUNT_COLUMNS,
    ESTIMATED_PERCENTAGE_COLUMNS,
)
from projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils import (
    IMPUTED_PERCENTAGE_COLUMNS,
    PERCENTAGE_COLUMNS,
    ROLLING_AVERAGE_PERCENTAGE_COLUMNS,
)
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusImputeTempColumns as ImputeTempCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.ascwds_labelled_vocab import (
    EmploymentStatusID,
    EmploymentStatusLabels,
    MainJobRoleID,
    MainJobRoleLabels,
    PublishedJobRoleLabels,
)
from utils.column_values.categorical_column_values import (
    EmploymentStatusEstimateSource,
    EmploymentStatusFilteringRule,
    JobGroupLabels,
    PrimaryServiceType,
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
class AddEstimatedEmploymentStatusColumnsTestCase:
    id: str
    input_data: dict[str, Any]
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


def _estimate_row(
    cleaned: list[float | None],
    imputed: list[float | None],
    rolling_avg: list[float | None],
    metric: float | None,
) -> dict[str, list]:
    """One row; each list holds the 5 statuses as perm, temp, bank, agency, other."""
    return {
        METRIC: [metric],
        **{col: [val] for col, val in zip(PERCENTAGE_COLUMNS, cleaned)},
        **{col: [val] for col, val in zip(IMPUTED_PERCENTAGE_COLUMNS, imputed)},
        **{
            col: [val]
            for col, val in zip(ROLLING_AVERAGE_PERCENTAGE_COLUMNS, rolling_avg)
        },
    }


def _estimate_expected(
    row: dict[str, list],
    estimated: list[float | None],
    source: str | None,
    counts: list[float | None],
    employees: float | None,
) -> dict[str, list]:
    return {
        **row,
        **{col: [val] for col, val in zip(ESTIMATED_PERCENTAGE_COLUMNS, estimated)},
        EmpStatus.percentage_estimate_source: [source],
        **{col: [val] for col, val in zip(ESTIMATED_COUNT_COLUMNS, counts)},
        EmpStatus.estimated_employees: [employees],
    }


NULLS = [None] * 5
CLEANED = [0.5, 0.25, 0.125, 0.0625, 0.0625]
IMPUTED = [0.4, 0.3, 0.1, 0.1, 0.1]
ROLLING_AVG = [0.3, 0.3, 0.2, 0.1, 0.1]


@dataclass
class TestEstimateUtilsData:
    add_estimated_employment_status_columns_test_cases = [
        AddEstimatedEmploymentStatusColumnsTestCase(
            id="uses_cleaned_percentages_when_populated",
            input_data=_estimate_row(CLEANED, IMPUTED, ROLLING_AVG, 64.0),
            expected_data=_estimate_expected(
                _estimate_row(CLEANED, IMPUTED, ROLLING_AVG, 64.0),
                CLEANED,
                EmploymentStatusEstimateSource.cleaned,
                [32.0, 16.0, 8.0, 4.0, 4.0],
                48.0,
            ),
        ),
        AddEstimatedEmploymentStatusColumnsTestCase(
            id="uses_imputed_percentages_when_cleaned_null",
            input_data=_estimate_row(NULLS, IMPUTED, ROLLING_AVG, 100.0),
            expected_data=_estimate_expected(
                _estimate_row(NULLS, IMPUTED, ROLLING_AVG, 100.0),
                IMPUTED,
                EmploymentStatusEstimateSource.imputed,
                [40.0, 30.0, 10.0, 10.0, 10.0],
                70.0,
            ),
        ),
        AddEstimatedEmploymentStatusColumnsTestCase(
            id="uses_rolling_average_percentages_when_cleaned_and_imputed_null",
            input_data=_estimate_row(NULLS, NULLS, ROLLING_AVG, 100.0),
            expected_data=_estimate_expected(
                _estimate_row(NULLS, NULLS, ROLLING_AVG, 100.0),
                ROLLING_AVG,
                EmploymentStatusEstimateSource.rolling_avg,
                [30.0, 30.0, 20.0, 10.0, 10.0],
                60.0,
            ),
        ),
        AddEstimatedEmploymentStatusColumnsTestCase(
            id="leaves_estimates_null_when_all_percentages_null",
            input_data=_estimate_row(NULLS, NULLS, NULLS, 100.0),
            expected_data=_estimate_expected(
                _estimate_row(NULLS, NULLS, NULLS, 100.0), NULLS, None, NULLS, None
            ),
        ),
        AddEstimatedEmploymentStatusColumnsTestCase(
            id="leaves_counts_null_when_metric_null",
            input_data=_estimate_row(CLEANED, IMPUTED, ROLLING_AVG, None),
            expected_data=_estimate_expected(
                _estimate_row(CLEANED, IMPUTED, ROLLING_AVG, None),
                CLEANED,
                EmploymentStatusEstimateSource.cleaned,
                NULLS,
                None,
            ),
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
TRENDLINE_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_imputed_for_trendline,
    EmpStatus.temporary_percentage_imputed_for_trendline,
    EmpStatus.bank_or_pool_percentage_imputed_for_trendline,
    EmpStatus.agency_percentage_imputed_for_trendline,
    EmpStatus.other_percentage_imputed_for_trendline,
]
ROLLING_AVERAGE_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_rolling_avg,
    EmpStatus.temporary_percentage_rolling_avg,
    EmpStatus.bank_or_pool_percentage_rolling_avg,
    EmpStatus.agency_percentage_rolling_avg,
    EmpStatus.other_percentage_rolling_avg,
]
FULL_IMPUTED_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_full_imputed,
    EmpStatus.temporary_percentage_full_imputed,
    EmpStatus.bank_or_pool_percentage_full_imputed,
    EmpStatus.agency_percentage_full_imputed,
    EmpStatus.other_percentage_full_imputed,
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
TEN_DAYS_APART = [date(2024, 1, 1) + timedelta(days=10 * i) for i in range(5)]
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
            **status_split(expected_permanent, TRENDLINE_PERCENTAGE_COLUMNS),
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
        **status_split(permanent, TRENDLINE_PERCENTAGE_COLUMNS),
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


def full_imputation_case(
    id: str,
    dates: list[date],
    permanent: list[Optional[float]],
    rolling_permanent: list[Optional[float]],
    expected_permanent: list[Optional[float]],
) -> ImputeUtilsTestCase:
    """
    Build a single location and job role case from the known permanent share, its rolling
    average and the expected full imputed share, splitting each with `status_split`.
    """
    input_data = {
        IndCQC.location_id: ["loc1"] * len(dates),
        IndCQC.published_job_role_label: [CARE_WORKER] * len(dates),
        IndCQC.cqc_location_import_date: dates,
        **status_split(permanent, PERCENTAGE_COLUMNS),
        **status_split(rolling_permanent, ROLLING_AVERAGE_PERCENTAGE_COLUMNS),
    }
    return ImputeUtilsTestCase(
        id=id,
        input_data=input_data,
        expected_data={
            **input_data,
            **status_split(expected_permanent, FULL_IMPUTED_PERCENTAGE_COLUMNS),
        },
    )


def full_imputation_five_status_case(
    id: str,
    dates: list[date],
    known: list[list[Optional[float]]],
    rolling: list[list[float]],
    expected: list[list[Optional[float]]],
) -> ImputeUtilsTestCase:
    """
    Build a single location and job role case that states every status. `known`, `rolling` and
    `expected` each hold one list per status, in permanent, temporary, bank or pool, agency,
    other order.
    """
    input_data = {
        IndCQC.location_id: ["loc1"] * len(dates),
        IndCQC.published_job_role_label: [CARE_WORKER] * len(dates),
        IndCQC.cqc_location_import_date: dates,
        **dict(zip(PERCENTAGE_COLUMNS, known)),
        **dict(zip(ROLLING_AVERAGE_PERCENTAGE_COLUMNS, rolling)),
    }
    return ImputeUtilsTestCase(
        id=id,
        input_data=input_data,
        expected_data={
            **input_data,
            **dict(zip(FULL_IMPUTED_PERCENTAGE_COLUMNS, expected)),
        },
    )


def full_imputation_rows_case(
    id: str,
    rows: list[tuple[str, str, date, Optional[float], float]],
    expected_permanent: list[Optional[float]],
) -> ImputeUtilsTestCase:
    """
    Build a case across locations and job roles from (location, job role, date, permanent
    share, rolling average permanent share) rows, splitting each share with `status_split`.
    """
    locations, roles, dates, permanent, rolling_permanent = map(list, zip(*rows))
    input_data = {
        IndCQC.location_id: locations,
        IndCQC.published_job_role_label: roles,
        IndCQC.cqc_location_import_date: dates,
        **status_split(permanent, PERCENTAGE_COLUMNS),
        **status_split(rolling_permanent, ROLLING_AVERAGE_PERCENTAGE_COLUMNS),
    }
    return ImputeUtilsTestCase(
        id=id,
        input_data=input_data,
        expected_data={
            **input_data,
            **status_split(expected_permanent, FULL_IMPUTED_PERCENTAGE_COLUMNS),
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
                EmpStatus.permanent_percentage_imputed_for_trendline: [0.5, 0.4, 0.2],
                EmpStatus.temporary_percentage_imputed_for_trendline: [0.1, 0.2, 0.4],
                EmpStatus.bank_or_pool_percentage_imputed_for_trendline: [
                    0.1,
                    0.1,
                    0.1,
                ],
                EmpStatus.agency_percentage_imputed_for_trendline: [0.2, 0.2, 0.2],
                EmpStatus.other_percentage_imputed_for_trendline: [0.1, 0.1, 0.1],
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
        # A Float32 sliding-window sum leaves a residual once non-zero values leave the
        # window, so windows of all zeros come out slightly above (or below) 0.
        rolling_average_case(
            id="does_not_carry_rounding_error_into_later_windows",
            rows=[
                ("loc1", NON_RES, LONDON, CARE_WORKER, date(2024, month, 1), value)
                for month, value in zip(
                    range(1, 11), [0.7, 0.1, 0.3, 0.9, 0.6, 0.2, 0.0, 0.0, 0.0, 0.0]
                )
            ],
            expected_permanent=[
                0.7,
                0.8 / 2,
                1.1 / 3,
                1.3 / 3,
                1.8 / 3,
                1.7 / 3,
                0.8 / 3,
                0.2 / 3,
                0.0,
                0.0,
            ],
        ),
    ]

    add_full_imputed_percentages_test_cases = [
        full_imputation_case(
            id="does_not_change_known_values",
            dates=FIVE_MONTHS,
            permanent=[0.2, 0.4, 0.6, 0.8, 0.5],
            rolling_permanent=[0.5] * 5,
            expected_permanent=[0.2, 0.4, 0.6, 0.8, 0.5],
        ),
        full_imputation_case(
            id="extrapolates_forwards_by_nominal_change_in_rolling_average",
            dates=FIVE_MONTHS,
            permanent=[0.2, None, None, None, None],
            rolling_permanent=[0.5, 0.6, 0.7, 0.55, 0.5],
            expected_permanent=[0.2, 0.3, 0.4, 0.25, 0.2],
        ),
        full_imputation_case(
            id="extrapolates_backwards_by_nominal_change_in_rolling_average",
            dates=FIVE_MONTHS,
            permanent=[None, None, 0.4, 0.5, 0.6],
            rolling_permanent=[0.35, 0.45, 0.5, 0.55, 0.6],
            expected_permanent=[0.25, 0.35, 0.4, 0.5, 0.6],
        ),
        full_imputation_case(
            id="interpolates_gap_along_rolling_average_trend",
            dates=[date(2024, 1, 1) + timedelta(days=d) for d in [0, 5, 10, 30, 40]],
            permanent=[0.2, None, None, None, 0.6],
            rolling_permanent=[0.5, 0.6, 0.6, 0.7, 0.8],
            expected_permanent=[0.2, 0.3125, 0.325, 0.475, 0.6],
        ),
        full_imputation_five_status_case(
            id="imputes_each_status_from_its_own_rolling_average",
            dates=FIVE_MONTHS[:2],
            known=[[0.4, None], [0.3, None], [0.1, None], [0.1, None], [0.1, None]],
            rolling=[[0.5, 0.45], [0.2, 0.25], [0.1, 0.1], [0.1, 0.12], [0.1, 0.08]],
            expected=[[0.4, 0.35], [0.3, 0.35], [0.1, 0.1], [0.1, 0.12], [0.1, 0.08]],
        ),
        full_imputation_five_status_case(
            id="floors_negative_values_at_zero_and_reshares_to_sum_to_one",
            dates=FIVE_MONTHS[:2],
            known=[[0.1, None], [0.3, None], [0.0, None], [0.6, None], [0.0, None]],
            rolling=[[0.5, 0.2], [0.2, 0.3], [0.0, 0.0], [0.3, 0.5], [0.0, 0.0]],
            expected=[
                [0.1, 0.0],
                [0.3, 1 / 3],
                [0.0, 0.0],
                [0.6, 2 / 3],
                [0.0, 0.0],
            ],
        ),
        full_imputation_case(
            id="leaves_null_when_group_has_no_known_values",
            dates=FIVE_MONTHS,
            permanent=[None] * 5,
            rolling_permanent=[0.3] * 5,
            expected_permanent=[None] * 5,
        ),
        full_imputation_rows_case(
            id="keeps_locations_and_job_roles_separate",
            rows=[
                ("loc1", CARE_WORKER, date(2024, 1, 1), 0.2, 0.5),
                ("loc1", REGISTERED_NURSE, date(2024, 1, 1), 0.6, 0.5),
                ("loc2", CARE_WORKER, date(2024, 1, 1), 0.4, 0.5),
                ("loc1", CARE_WORKER, date(2024, 2, 1), None, 0.6),
                ("loc1", REGISTERED_NURSE, date(2024, 2, 1), None, 0.3),
                ("loc2", CARE_WORKER, date(2024, 2, 1), None, 0.6),
            ],
            expected_permanent=[0.2, 0.6, 0.4, 0.3, 0.4, 0.5],
        ),
        full_imputation_case(
            id="drops_temporary_columns",
            dates=TEN_DAYS_APART,
            permanent=[None, 0.2, None, 0.4, None],
            rolling_permanent=[0.5] * 5,
            expected_permanent=[0.2, 0.2, 0.3, 0.4, 0.4],
        ),
        full_imputation_case(
            id="fills_from_a_single_known_value",
            dates=FIVE_MONTHS,
            permanent=[None, None, 0.4, None, None],
            rolling_permanent=[0.35, 0.45, 0.5, 0.6, 0.55],
            expected_permanent=[0.25, 0.35, 0.4, 0.5, 0.45],
        ),
        full_imputation_case(
            id="fills_across_gaps_of_many_years",
            dates=[
                date(2013, 1, 1),
                date(2018, 1, 1),
                date(2023, 1, 1),
                date(2026, 1, 1),
            ],
            permanent=[0.2, None, 0.6, None],
            rolling_permanent=[0.5, 0.6, 0.8, 0.7],
            expected_permanent=[0.2, 0.35, 0.6, 0.5],
        ),
        full_imputation_case(
            id="leaves_null_when_rolling_average_is_null",
            dates=FIVE_MONTHS[:3],
            permanent=[0.2, None, None],
            rolling_permanent=[0.5, None, 0.6],
            expected_permanent=[0.2, None, 0.3],
        ),
    ]


CLEAN_UTILS_IMPORT_DATE = date(2024, 1, 1)


@dataclass
class CleanUtilsTestCase:

    id: str

    input_data: dict[str, Any]

    expected_data: dict[str, Any]


def _raw_counts_input(rows: list[tuple]) -> dict:
    """Builds raw-count input columns from (org, location, role, date, perm,

    temp, bank_or_pool, agency, other) rows, using date for both import dates."""

    columns = list(zip(*rows))

    return {
        IndCQC.organisation_id: list(columns[0]),
        IndCQC.location_id: list(columns[1]),
        IndCQC.published_job_role_label: list(columns[2]),
        IndCQC.cqc_location_import_date: list(columns[3]),
        IndCQC.ascwds_workplace_import_date: list(columns[3]),
        EmpStatus.permanent_count: list(columns[4]),
        EmpStatus.temporary_count: list(columns[5]),
        EmpStatus.bank_or_pool_count: list(columns[6]),
        EmpStatus.agency_count: list(columns[7]),
        EmpStatus.other_count: list(columns[8]),
    }


@dataclass
class TestCleanUtilsData:

    dedup_then_org_ratio_test_cases = [
        CleanUtilsTestCase(
            id="org_uses_raw_counts_and_dedup_judges_whole_workplace_stale",
            input_data=_raw_counts_input(
                [
                    # loc1 role A changes on the 2nd date but role B doesn't, so
                    # the workplace isn't stale and both roles keep their counts.
                    ("org1", "loc1", "A", CLEAN_UTILS_IMPORT_DATE, 0, 0, 0, 0, 4),
                    ("org1", "loc1", "A", date(2024, 2, 1), 1, 0, 0, 0, 4),
                    ("org1", "loc1", "B", CLEAN_UTILS_IMPORT_DATE, 0, 0, 0, 0, 5),
                    ("org1", "loc1", "B", date(2024, 2, 1), 0, 0, 0, 0, 5),
                    # loc2 is unchanged in every role, so it's stale (null dedup)
                    # but its raw staff still count towards the org total.
                    ("org1", "loc2", "A", CLEAN_UTILS_IMPORT_DATE, 0, 0, 0, 0, 5),
                    ("org1", "loc2", "A", date(2024, 2, 1), 0, 0, 0, 0, 5),
                    ("org1", "loc2", "B", CLEAN_UTILS_IMPORT_DATE, 0, 0, 0, 0, 5),
                    ("org1", "loc2", "B", date(2024, 2, 1), 0, 0, 0, 0, 5),
                ]
            ),
            # Raw org total on the 2nd date is 20 staff with 1 permanent (0.05),
            # so the org fails. Dedup counts alone (loc1 only: 1/10) would pass.
            expected_data={
                IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
                IndCQC.published_job_role_label: ["A", "B", "A", "B"],
                EmpStatus.permanent_count_clean: [None] * 4,
                EmpStatus.other_count_clean: [None] * 4,
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.missing_data,
                    EmploymentStatusFilteringRule.missing_data,
                ],
            },
        ),
    ]

    dedup_then_location_ratio_test_cases = [
        CleanUtilsTestCase(
            id="location_keeps_all_roles_when_any_role_changed_in_the_workplace",
            input_data=_raw_counts_input(
                [
                    # Role A changes on the 2nd date, role B doesn't. Workplace
                    # dedup keeps both roles, so the location ratio uses both
                    # (10 of 20 permanent/temporary). Per-role dedup would null
                    # role B and wrongly flag the location on role A alone.
                    ("org2", "loc3", "A", CLEAN_UTILS_IMPORT_DATE, 0, 0, 0, 0, 9),
                    ("org2", "loc3", "A", date(2024, 2, 1), 0, 0, 0, 0, 10),
                    ("org2", "loc3", "B", CLEAN_UTILS_IMPORT_DATE, 5, 5, 0, 0, 0),
                    ("org2", "loc3", "B", date(2024, 2, 1), 5, 5, 0, 0, 0),
                ]
            ),
            expected_data={
                IndCQC.location_id: ["loc3", "loc3"],
                IndCQC.published_job_role_label: ["A", "B"],
                EmpStatus.permanent_count_clean: [0, 5],
                EmpStatus.other_count_clean: [10, 0],
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 2,
            },
        ),
    ]

    null_counts_for_low_location_ratio_test_cases = [
        CleanUtilsTestCase(
            id="sums_permanent_and_temporary_across_job_roles_before_comparing_to_location_staff",
            input_data={
                IndCQC.location_id: [
                    "loc_low_ratio",
                    "loc_low_ratio",
                    "loc_high_ratio",
                    "loc_high_ratio",
                    "loc_small_ok",
                ],
                IndCQC.establishment_id: [
                    "est_low_ratio",
                    "est_low_ratio",
                    "est_high_ratio",
                    "est_high_ratio",
                    "est_small_ok",
                ],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 5,
                EmpStatus.permanent_count_dedup: [0, 0, 1, 0, 1],
                EmpStatus.temporary_count_dedup: [0, 0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0, 0, 0, 0],
                EmpStatus.agency_count_dedup: [0, 0, 0, 0, 0],
                EmpStatus.other_count_dedup: [10, 10, 9, 10, 4],
                EmpStatus.permanent_count_clean: [0, 0, 1, 0, 1],
                EmpStatus.temporary_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.agency_count_clean: [0, 0, 0, 0, 0],
                EmpStatus.other_count_clean: [10, 10, 9, 10, 4],
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 5,
            },
            expected_data={
                IndCQC.location_id: [
                    "loc_low_ratio",
                    "loc_low_ratio",
                    "loc_high_ratio",
                    "loc_high_ratio",
                    "loc_small_ok",
                ],
                IndCQC.establishment_id: [
                    "est_low_ratio",
                    "est_low_ratio",
                    "est_high_ratio",
                    "est_high_ratio",
                    "est_small_ok",
                ],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 5,
                EmpStatus.permanent_count_dedup: [0, 0, 1, 0, 1],
                EmpStatus.temporary_count_dedup: [0, 0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0, 0, 0, 0],
                EmpStatus.agency_count_dedup: [0, 0, 0, 0, 0],
                EmpStatus.other_count_dedup: [10, 10, 9, 10, 4],
                EmpStatus.permanent_count_clean: [None, None, 1, 0, 1],
                EmpStatus.temporary_count_clean: [None, None, 0, 0, 0],
                EmpStatus.bank_or_pool_count_clean: [None, None, 0, 0, 0],
                EmpStatus.agency_count_clean: [None, None, 0, 0, 0],
                EmpStatus.other_count_clean: [None, None, 9, 10, 4],
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
            id="uses_dedup_counts_not_raw_counts_for_the_ratio",
            input_data={
                # loc_a: raw ratio is fine (0.5) but dedup is 0, so it's nulled.
                # loc_b: raw ratio is 0 but dedup is 0.1, so it's kept.
                IndCQC.location_id: ["loc_a", "loc_b"],
                IndCQC.establishment_id: ["est_a", "est_b"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [5, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [5, 10],
                EmpStatus.permanent_count_dedup: [0, 1],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count_dedup: [10, 9],
                EmpStatus.permanent_count_clean: [0, 1],
                EmpStatus.temporary_count_clean: [0, 0],
                EmpStatus.bank_or_pool_count_clean: [0, 0],
                EmpStatus.agency_count_clean: [0, 0],
                EmpStatus.other_count_clean: [10, 9],
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 2,
            },
            expected_data={
                IndCQC.location_id: ["loc_a", "loc_b"],
                IndCQC.establishment_id: ["est_a", "est_b"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [5, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [5, 10],
                EmpStatus.permanent_count_dedup: [0, 1],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count_dedup: [10, 9],
                EmpStatus.permanent_count_clean: [None, 1],
                EmpStatus.temporary_count_clean: [None, 0],
                EmpStatus.bank_or_pool_count_clean: [None, 0],
                EmpStatus.agency_count_clean: [None, 0],
                EmpStatus.other_count_clean: [None, 9],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.location_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.populated,
                ],
            },
        ),
        CleanUtilsTestCase(
            id="keeps_org_level_reason_when_location_ratio_also_fails",
            input_data={
                IndCQC.location_id: ["loc_org_nulled"],
                IndCQC.establishment_id: ["est_org_nulled"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                EmpStatus.permanent_count_dedup: [0],
                EmpStatus.temporary_count_dedup: [0],
                EmpStatus.bank_or_pool_count_dedup: [0],
                EmpStatus.agency_count_dedup: [0],
                EmpStatus.other_count_dedup: [20],
                EmpStatus.permanent_count_clean: [None],
                EmpStatus.temporary_count_clean: [None],
                EmpStatus.bank_or_pool_count_clean: [None],
                EmpStatus.agency_count_clean: [None],
                EmpStatus.other_count_clean: [None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ],
            },
            expected_data={
                IndCQC.location_id: ["loc_org_nulled"],
                IndCQC.establishment_id: ["est_org_nulled"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                EmpStatus.permanent_count_dedup: [0],
                EmpStatus.temporary_count_dedup: [0],
                EmpStatus.bank_or_pool_count_dedup: [0],
                EmpStatus.agency_count_dedup: [0],
                EmpStatus.other_count_dedup: [20],
                EmpStatus.permanent_count_clean: [None],
                EmpStatus.temporary_count_clean: [None],
                EmpStatus.bank_or_pool_count_clean: [None],
                EmpStatus.agency_count_clean: [None],
                EmpStatus.other_count_clean: [None],
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
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.permanent_count_dedup: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [9, 10],
                EmpStatus.other_count_dedup: [9, 10],
            },
            expected_data={
                IndCQC.organisation_id: ["org1", "org1"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.permanent_count_dedup: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [9, 10],
                EmpStatus.other_count_dedup: [9, 10],
                EmpStatus.permanent_count_clean: [None, None],
                EmpStatus.temporary_count_clean: [None, None],
                EmpStatus.bank_or_pool_count_clean: [None, None],
                EmpStatus.agency_count_clean: [None, None],
                EmpStatus.other_count_clean: [None, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ]
                * 2,
            },
        ),
        CleanUtilsTestCase(
            id="nulls_small_org_with_no_permanent_or_temporary_staff",
            input_data={
                # No minimum size: 4 staff and none permanent/temporary fails.
                IndCQC.organisation_id: ["org2"] * 4,
                IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
                IndCQC.establishment_id: ["est1", "est1", "est2", "est2"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 4,
                EmpStatus.permanent_count: [0, 0, 0, 0],
                EmpStatus.permanent_count_dedup: [0, 0, 0, 0],
                EmpStatus.temporary_count: [0, 0, 0, 0],
                EmpStatus.temporary_count_dedup: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0, 0, 0],
                EmpStatus.agency_count: [0, 0, 0, 0],
                EmpStatus.agency_count_dedup: [0, 0, 0, 0],
                EmpStatus.other_count: [1, 1, 1, 1],
                EmpStatus.other_count_dedup: [1, 1, 1, 1],
            },
            expected_data={
                IndCQC.organisation_id: ["org2"] * 4,
                IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
                IndCQC.establishment_id: ["est1", "est1", "est2", "est2"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 4,
                EmpStatus.permanent_count: [0, 0, 0, 0],
                EmpStatus.permanent_count_dedup: [0, 0, 0, 0],
                EmpStatus.temporary_count: [0, 0, 0, 0],
                EmpStatus.temporary_count_dedup: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count: [0, 0, 0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0, 0, 0],
                EmpStatus.agency_count: [0, 0, 0, 0],
                EmpStatus.agency_count_dedup: [0, 0, 0, 0],
                EmpStatus.other_count: [1, 1, 1, 1],
                EmpStatus.other_count_dedup: [1, 1, 1, 1],
                EmpStatus.permanent_count_clean: [None] * 4,
                EmpStatus.temporary_count_clean: [None] * 4,
                EmpStatus.bank_or_pool_count_clean: [None] * 4,
                EmpStatus.agency_count_clean: [None] * 4,
                EmpStatus.other_count_clean: [None] * 4,
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio
                ]
                * 4,
            },
        ),
        CleanUtilsTestCase(
            id="does_not_null_org_with_ratio_above_threshold",
            input_data={
                IndCQC.organisation_id: ["org3"],
                IndCQC.location_id: ["loc1"],
                IndCQC.establishment_id: ["est1"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                EmpStatus.permanent_count: [5],
                EmpStatus.permanent_count_dedup: [5],
                EmpStatus.temporary_count: [5],
                EmpStatus.temporary_count_dedup: [5],
                EmpStatus.bank_or_pool_count: [0],
                EmpStatus.bank_or_pool_count_dedup: [0],
                EmpStatus.agency_count: [0],
                EmpStatus.agency_count_dedup: [0],
                EmpStatus.other_count: [0],
                EmpStatus.other_count_dedup: [0],
            },
            expected_data={
                IndCQC.organisation_id: ["org3"],
                IndCQC.location_id: ["loc1"],
                IndCQC.establishment_id: ["est1"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE],
                EmpStatus.permanent_count: [5],
                EmpStatus.permanent_count_dedup: [5],
                EmpStatus.temporary_count: [5],
                EmpStatus.temporary_count_dedup: [5],
                EmpStatus.bank_or_pool_count: [0],
                EmpStatus.bank_or_pool_count_dedup: [0],
                EmpStatus.agency_count: [0],
                EmpStatus.agency_count_dedup: [0],
                EmpStatus.other_count: [0],
                EmpStatus.other_count_dedup: [0],
                EmpStatus.permanent_count_clean: [5],
                EmpStatus.temporary_count_clean: [5],
                EmpStatus.bank_or_pool_count_clean: [0],
                EmpStatus.agency_count_clean: [0],
                EmpStatus.other_count_clean: [0],
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
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [0, 0],
                EmpStatus.permanent_count_dedup: [0, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [20, 20],
                EmpStatus.other_count_dedup: [20, 20],
            },
            expected_data={
                IndCQC.organisation_id: [None, None],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [0, 0],
                EmpStatus.permanent_count_dedup: [0, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [20, 20],
                EmpStatus.other_count_dedup: [20, 20],
                EmpStatus.permanent_count_clean: [0, 0],
                EmpStatus.temporary_count_clean: [0, 0],
                EmpStatus.bank_or_pool_count_clean: [0, 0],
                EmpStatus.agency_count_clean: [0, 0],
                EmpStatus.other_count_clean: [20, 20],
                EmpStatus.filtering_rule: [EmploymentStatusFilteringRule.populated] * 2,
            },
        ),
        CleanUtilsTestCase(
            id="flags_missing_data_for_a_null_row_even_when_the_orgs_overall_ratio_is_fine",
            input_data={
                # loc1's 2nd job role has no worker records (null raw and
                # dedup), but its 1st job role alone keeps the org ratio fine
                # - the null row must not default to "populated". The null
                # row also drops out of org_total_staff entirely rather than
                # counting as unstaffed.
                IndCQC.organisation_id: ["org4", "org4"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [10, None],
                EmpStatus.permanent_count_dedup: [10, None],
                EmpStatus.temporary_count: [0, None],
                EmpStatus.temporary_count_dedup: [0, None],
                EmpStatus.bank_or_pool_count: [0, None],
                EmpStatus.bank_or_pool_count_dedup: [0, None],
                EmpStatus.agency_count: [0, None],
                EmpStatus.agency_count_dedup: [0, None],
                EmpStatus.other_count: [0, None],
                EmpStatus.other_count_dedup: [0, None],
            },
            expected_data={
                IndCQC.organisation_id: ["org4", "org4"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [10, None],
                EmpStatus.permanent_count_dedup: [10, None],
                EmpStatus.temporary_count: [0, None],
                EmpStatus.temporary_count_dedup: [0, None],
                EmpStatus.bank_or_pool_count: [0, None],
                EmpStatus.bank_or_pool_count_dedup: [0, None],
                EmpStatus.agency_count: [0, None],
                EmpStatus.agency_count_dedup: [0, None],
                EmpStatus.other_count: [0, None],
                EmpStatus.other_count_dedup: [0, None],
                EmpStatus.permanent_count_clean: [10, None],
                EmpStatus.temporary_count_clean: [0, None],
                EmpStatus.bank_or_pool_count_clean: [0, None],
                EmpStatus.agency_count_clean: [0, None],
                EmpStatus.other_count_clean: [0, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.populated,
                    EmploymentStatusFilteringRule.missing_data,
                ],
            },
        ),
        CleanUtilsTestCase(
            id="counts_unchanged_since_last_snapshot_towards_org_staff_total",
            input_data={
                # loc2's counts are unchanged since the last snapshot (null
                # dedup) but still real staff: raw gives an org total of 20
                # and a 0.05 ratio, so the org is nulled. Dedup alone would
                # see only loc1's 10 staff at a 0.1 ratio and keep it.
                IndCQC.organisation_id: ["org7", "org7"],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [9, 10],
                EmpStatus.permanent_count_dedup: [1, None],
                EmpStatus.temporary_count_dedup: [0, None],
                EmpStatus.bank_or_pool_count_dedup: [0, None],
                EmpStatus.agency_count_dedup: [0, None],
                EmpStatus.other_count_dedup: [9, None],
            },
            expected_data={
                IndCQC.organisation_id: ["org7", "org7"],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.other_count: [9, 10],
                EmpStatus.permanent_count_dedup: [1, None],
                EmpStatus.temporary_count_dedup: [0, None],
                EmpStatus.bank_or_pool_count_dedup: [0, None],
                EmpStatus.agency_count_dedup: [0, None],
                EmpStatus.other_count_dedup: [9, None],
                EmpStatus.permanent_count_clean: [None, None],
                EmpStatus.temporary_count_clean: [None, None],
                EmpStatus.bank_or_pool_count_clean: [None, None],
                EmpStatus.agency_count_clean: [None, None],
                EmpStatus.other_count_clean: [None, None],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio,
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
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.permanent_count_dedup: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [9, 10],
                EmpStatus.other_count_dedup: [9, 10],
            },
            expected_data={
                IndCQC.organisation_id: ["org5", "org5"],
                IndCQC.location_id: ["loc1", "loc2"],
                IndCQC.establishment_id: ["est1", "est2"],
                IndCQC.ascwds_workplace_import_date: [CLEAN_UTILS_IMPORT_DATE] * 2,
                EmpStatus.permanent_count: [1, 0],
                EmpStatus.permanent_count_dedup: [1, 0],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [9, 10],
                EmpStatus.other_count_dedup: [9, 10],
                EmpStatus.permanent_count_clean: [None, None],
                EmpStatus.temporary_count_clean: [None, None],
                EmpStatus.bank_or_pool_count_clean: [None, None],
                EmpStatus.agency_count_clean: [None, None],
                EmpStatus.other_count_clean: [None, None],
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
                IndCQC.ascwds_workplace_import_date: [
                    CLEAN_UTILS_IMPORT_DATE,
                    date(2024, 2, 1),
                ],
                EmpStatus.permanent_count: [0, 15],
                EmpStatus.permanent_count_dedup: [0, 15],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [20, 5],
                EmpStatus.other_count_dedup: [20, 5],
            },
            expected_data={
                IndCQC.organisation_id: ["org6", "org6"],
                IndCQC.location_id: ["loc1", "loc1"],
                IndCQC.establishment_id: ["est1", "est1"],
                IndCQC.ascwds_workplace_import_date: [
                    CLEAN_UTILS_IMPORT_DATE,
                    date(2024, 2, 1),
                ],
                EmpStatus.permanent_count: [0, 15],
                EmpStatus.permanent_count_dedup: [0, 15],
                EmpStatus.temporary_count: [0, 0],
                EmpStatus.temporary_count_dedup: [0, 0],
                EmpStatus.bank_or_pool_count: [0, 0],
                EmpStatus.bank_or_pool_count_dedup: [0, 0],
                EmpStatus.agency_count: [0, 0],
                EmpStatus.agency_count_dedup: [0, 0],
                EmpStatus.other_count: [20, 5],
                EmpStatus.other_count_dedup: [20, 5],
                EmpStatus.permanent_count_clean: [None, 15],
                EmpStatus.temporary_count_clean: [None, 0],
                EmpStatus.bank_or_pool_count_clean: [None, 0],
                EmpStatus.agency_count_clean: [None, 0],
                EmpStatus.other_count_clean: [None, 5],
                EmpStatus.filtering_rule: [
                    EmploymentStatusFilteringRule.org_level_low_permanent_temporary_ratio,
                    EmploymentStatusFilteringRule.populated,
                ],
            },
        ),
    ]
