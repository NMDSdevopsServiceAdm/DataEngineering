from dataclasses import dataclass

from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)


@dataclass
class AscwdsWorkerValueLabelsJobGroup:
    """A dict where keys = job role and values = job group"""

    job_role_to_job_group_dict = {
        MainJobRoleLabels.senior_management: JobGroupLabels.managers,
        MainJobRoleLabels.middle_management: JobGroupLabels.managers,
        MainJobRoleLabels.first_line_manager: JobGroupLabels.managers,
        MainJobRoleLabels.registered_manager: JobGroupLabels.managers,
        MainJobRoleLabels.supervisor: JobGroupLabels.managers,
        MainJobRoleLabels.social_worker: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.senior_care_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.care_worker: JobGroupLabels.direct_care,
        MainJobRoleLabels.community_support_and_outreach: JobGroupLabels.direct_care,
        MainJobRoleLabels.employment_support: JobGroupLabels.direct_care,
        MainJobRoleLabels.advocacy: JobGroupLabels.direct_care,
        MainJobRoleLabels.occupational_therapist: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.registered_nurse: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.allied_health_professional: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.technician: JobGroupLabels.direct_care,
        MainJobRoleLabels.other_care_role: JobGroupLabels.direct_care,
        MainJobRoleLabels.other_managerial_staff: JobGroupLabels.managers,
        MainJobRoleLabels.admin_staff: JobGroupLabels.other,
        MainJobRoleLabels.ancillary_staff: JobGroupLabels.other,
        MainJobRoleLabels.other_non_care_related_staff: JobGroupLabels.other,
        MainJobRoleLabels.activites_worker: JobGroupLabels.other,
        MainJobRoleLabels.safeguarding_officer: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.occupational_therapist_assistant: JobGroupLabels.other,
        MainJobRoleLabels.registered_nursing_associate: JobGroupLabels.regulated_professions,
        MainJobRoleLabels.nursing_assistant: JobGroupLabels.direct_care,
        MainJobRoleLabels.assessment_officer: JobGroupLabels.other,
        MainJobRoleLabels.care_coordinator: JobGroupLabels.other,
        MainJobRoleLabels.childrens_roles: JobGroupLabels.other,
        MainJobRoleLabels.deputy_manager: JobGroupLabels.managers,
        MainJobRoleLabels.learning_and_development_lead: JobGroupLabels.other,
        MainJobRoleLabels.team_leader: JobGroupLabels.managers,
        MainJobRoleLabels.data_analyst: JobGroupLabels.other,
        MainJobRoleLabels.data_governance_manager: JobGroupLabels.managers,
        MainJobRoleLabels.it_and_digital_support: JobGroupLabels.other,
        MainJobRoleLabels.it_manager: JobGroupLabels.managers,
        MainJobRoleLabels.it_service_desk_manager: JobGroupLabels.managers,
        MainJobRoleLabels.software_developer: JobGroupLabels.other,
        MainJobRoleLabels.support_worker: JobGroupLabels.direct_care,
    }
