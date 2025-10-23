from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)


@dataclass
class AscwdsWorkerValueLabelsJobGroup:
    """A dict where keys = job role and values = job group"""

    column_name: str = IndCQC.main_job_group_labelled

    job_role_to_job_group_dict = {
        MainJobRoleLabels.advocacy: JobGroupLabels.direct_care,
        MainJobRoleLabels.care_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.community_support_and_outreach: JobGroupLabels.direct_care,
        MainJobRoleLabels.employment_support: JobGroupLabels.direct_care,
        MainJobRoleLabels.nursing_assistant: JobGroupLabels.direct_care,
        MainJobRoleLabels.other_care_role: JobGroupLabels.direct_care,
        MainJobRoleLabels.senior_care_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.support_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.technician: JobGroupLabels.direct_care,
        MainJobRoleLabels.data_governance_manager: JobGroupLabels.managers,
        MainJobRoleLabels.deputy_manager: JobGroupLabels.managers,
        MainJobRoleLabels.first_line_manager: JobGroupLabels.managers,
        MainJobRoleLabels.it_manager: JobGroupLabels.managers,
        MainJobRoleLabels.it_service_desk_manager: JobGroupLabels.managers,
        MainJobRoleLabels.middle_management: JobGroupLabels.managers,
        MainJobRoleLabels.other_managerial_staff: JobGroupLabels.managers,
        MainJobRoleLabels.registered_manager: JobGroupLabels.managers,
        MainJobRoleLabels.senior_management: JobGroupLabels.managers,
        MainJobRoleLabels.supervisor: JobGroupLabels.managers,
        MainJobRoleLabels.team_leader: JobGroupLabels.managers,
        MainJobRoleLabels.allied_health_professional: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.occupational_therapist: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.registered_nurse: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.registered_nursing_associate: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.safeguarding_officer: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.social_worker: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.activites_worker: JobGroupLabels.other,
        MainJobRoleLabels.admin_staff: JobGroupLabels.other,
        MainJobRoleLabels.ancillary_staff: JobGroupLabels.other,
        MainJobRoleLabels.assessment_officer: JobGroupLabels.other,
        MainJobRoleLabels.care_coordinator: JobGroupLabels.other,
        MainJobRoleLabels.childrens_roles: JobGroupLabels.other,
        MainJobRoleLabels.data_analyst: JobGroupLabels.other,
        MainJobRoleLabels.it_and_digital_support: JobGroupLabels.other,
        MainJobRoleLabels.learning_and_development_lead: JobGroupLabels.other,
        MainJobRoleLabels.occupational_therapist_assistant: JobGroupLabels.other,
        MainJobRoleLabels.other_non_care_related_staff: JobGroupLabels.other,
        MainJobRoleLabels.software_developer: JobGroupLabels.other,
    }
