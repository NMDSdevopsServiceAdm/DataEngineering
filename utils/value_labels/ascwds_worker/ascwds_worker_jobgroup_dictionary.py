from dataclasses import dataclass

from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)


@dataclass
class AscwdsWorkerValueLabelsJobGroup:
    """Class that provides job role to job group dicts.

    It also provides the following convenience methods to filter job roles:
        - `filter_roles`: Filter roles by job group.
        - `manager_roles`: Return the list of roles in the "managers" job group.
    """

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

    @classmethod
    def filter_roles(cls, group_label: str) -> list[str]:
        """Filter for job roles that match the group label."""
        d = cls.job_role_to_job_group_dict
        return [role for role, group in d.items() if group == group_label]

    @classmethod
    def manager_roles(cls) -> list[str]:
        """Return a list of roles in the "managers" job group."""
        return cls.filter_roles(JobGroupLabels.managers)
