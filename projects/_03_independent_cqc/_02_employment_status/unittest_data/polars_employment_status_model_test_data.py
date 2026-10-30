from dataclasses import dataclass
from datetime import date
from typing import Any

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_values.ascwds_labelled_vocab import PublishedJobRoleLabels
from utils.column_values.categorical_column_values import (
    RUI,
    CareHome,
    CurrentCSSR,
    ImputationRowKind,
    PrimaryServiceType,
    Region,
    Services,
    Specialisms,
)

KNOWN_SHARE = EmpStatus.permanent_pct
IMPUTED_SHARE = EmpStatus.permanent_pct_imputed
ROLLING_AVERAGE_SHARE = EmpStatus.permanent_pct_rolling_avg

CARE_WORKER = PublishedJobRoleLabels.care_worker
REGISTERED_NURSE = PublishedJobRoleLabels.registered_nurse
KNOWN = ImputationRowKind.known
INTERPOLATED = ImputationRowKind.interpolated
CARRIED = ImputationRowKind.carried

JAN_TO_JUN = [date(2024, month, 1) for month in range(1, 7)]
JAN_TO_MAR = JAN_TO_JUN[:3]


@dataclass
class ModelUtilsTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]


@dataclass
class TestModelUtilsData:
    add_imputation_row_kind_test_cases = [
        ModelUtilsTestCase(
            id="labels_every_kind_along_a_location_roles_dates",
            input_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 6,
                IndCQC.cqc_location_import_date: JAN_TO_JUN,
                KNOWN_SHARE: [None, 0.5, None, 0.7, None, None],
                IMPUTED_SHARE: [0.5, 0.5, 0.6, 0.7, 0.7, None],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 6,
                IndCQC.cqc_location_import_date: JAN_TO_JUN,
                KNOWN_SHARE: [None, 0.5, None, 0.7, None, None],
                IMPUTED_SHARE: [0.5, 0.5, 0.6, 0.7, 0.7, None],
                ShareModel.imputation_row_kind: [
                    CARRIED,
                    KNOWN,
                    INTERPOLATED,
                    KNOWN,
                    CARRIED,
                    None,
                ],
            },
        ),
        ModelUtilsTestCase(
            id="uses_each_location_roles_own_known_dates",
            input_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 3
                + [REGISTERED_NURSE] * 3,
                IndCQC.cqc_location_import_date: JAN_TO_MAR * 2,
                KNOWN_SHARE: [0.5, None, 0.7, None, 0.4, None],
                IMPUTED_SHARE: [0.5, 0.6, 0.7, 0.4, 0.4, 0.4],
            },
            expected_data={
                IndCQC.location_id: ["loc1"] * 6,
                IndCQC.published_job_role_label: [CARE_WORKER] * 3
                + [REGISTERED_NURSE] * 3,
                IndCQC.cqc_location_import_date: JAN_TO_MAR * 2,
                KNOWN_SHARE: [0.5, None, 0.7, None, 0.4, None],
                IMPUTED_SHARE: [0.5, 0.6, 0.7, 0.4, 0.4, 0.4],
                ShareModel.imputation_row_kind: [
                    KNOWN,
                    INTERPOLATED,
                    KNOWN,
                    CARRIED,
                    KNOWN,
                    CARRIED,
                ],
            },
        ),
    ]

    # loc1 has two roles and two dates, so a bad join would duplicate its rows.
    build_modelling_dataset_shares_data = {
        IndCQC.location_id: ["loc1"] * 4 + ["loc2"] * 2,
        IndCQC.cqc_location_import_date: JAN_TO_MAR[:2] * 3,
        IndCQC.published_job_role_label: [CARE_WORKER] * 2
        + [REGISTERED_NURSE] * 2
        + [CARE_WORKER] * 2,
        IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 6,
        IndCQC.care_home: [CareHome.not_care_home] * 6,
        IndCQC.current_region: [Region.london] * 6,
        IndCQC.services_offered: [[Services.domiciliary_care_service]] * 6,
        IndCQC.estimate_filled_posts_by_job_role: [8.0, 9.0, 2.0, 2.0, 5.0, 5.0],
        KNOWN_SHARE: [0.5, None, 0.6, None, None, None],
        IMPUTED_SHARE: [0.5, 0.5, 0.6, 0.6, None, None],
        ROLLING_AVERAGE_SHARE: [0.55] * 6,
    }
    build_modelling_dataset_estimates_data = {
        IndCQC.location_id: ["loc1", "loc1", "loc2", "loc2"],
        IndCQC.cqc_location_import_date: JAN_TO_MAR[:2] * 2,
        IndCQC.specialisms_offered: [[Specialisms.dementia]] * 4,
        IndCQC.regulated_activities_offered: [["Personal care"]] * 4,
        IndCQC.current_rural_urban_indicator_2011: [RUI.urban_city] * 4,
        IndCQC.current_cssr: [CurrentCSSR.leeds] * 4,
        IndCQC.time_registered: [12, 13, 40, 41],
        IndCQC.estimate_filled_posts: [10.0, 11.0, 5.0, 5.0],
    }
