from dataclasses import dataclass
from utils.column_values.categorical_column_values import MainJobRoleLabels

# Roy - Note - We have a dict in which the keys are job roles and values are counts per establishemnt.
#              But it only includes job roles that exist at that establishment.
#              If we need it to include all possible job roles (and set ones the establishment doesn't have to 0)
#              Then I thought we could apply this template to all establishments, then update it with the actual counts.


@dataclass
class TemplateJobroleCountDict:
    {
        MainJobRoleLabels.not_known: 0,
        MainJobRoleLabels.senior_management: 0,
        MainJobRoleLabels.middle_management: 0,
        MainJobRoleLabels.first_line_manager: 0,
        MainJobRoleLabels.registered_manager: 0,
        MainJobRoleLabels.supervisor: 0,
        MainJobRoleLabels.social_worker: 0,
        MainJobRoleLabels.senior_care_worker: 0,
        MainJobRoleLabels.care_worker: 0,
        MainJobRoleLabels.community_support_and_outreach: 0,
        MainJobRoleLabels.employment_support: 0,
        MainJobRoleLabels.advocacy: 0,
        MainJobRoleLabels.occupational_therapist: 0,
        MainJobRoleLabels.registered_nurse: 0,
        MainJobRoleLabels.allied_health_professional: 0,
        MainJobRoleLabels.technician: 0,
        MainJobRoleLabels.other_care_role: 0,
        MainJobRoleLabels.care_related_staff: 0,
        MainJobRoleLabels.admin_staff: 0,
        MainJobRoleLabels.ancillary_staff: 0,
        MainJobRoleLabels.other_non_care_related_staff: 0,
        MainJobRoleLabels.activites_worker: 0,
        MainJobRoleLabels.safeguarding_officer: 0,
        MainJobRoleLabels.occupational_therapist_assistant: 0,
        MainJobRoleLabels.registered_nursing_associate: 0,
        MainJobRoleLabels.nursing_assistant: 0,
        MainJobRoleLabels.assessment_officer: 0,
        MainJobRoleLabels.care_coordinator: 0,
        MainJobRoleLabels.childrens_roles: 0,
        MainJobRoleLabels.deputy_manager: 0,
        MainJobRoleLabels.learning_and_development_lead: 0,
        MainJobRoleLabels.team_leader: 0,
        MainJobRoleLabels.data_analyst: 0,
        MainJobRoleLabels.data_governance_manager: 0,
        MainJobRoleLabels.it_and_digital_support: 0,
        MainJobRoleLabels.it_manager: 0,
        MainJobRoleLabels.it_service_desk_manager: 0,
        MainJobRoleLabels.software_developer: 0,
        MainJobRoleLabels.support_worker: 0,
    }
