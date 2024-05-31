from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned_values import (
    AscwdsWorkerCleanedColumns as AWKClean,
)

from utils.column_values.cqc_providers_values import Sector


@dataclass
class MainJobRole:
    """The possible values of the main job role column in ASCWDS data"""

    column_name: str = AWKClean.main_job_role_labelled

    not_known: str = "Not known"
    senior_management: str = "Senior Management"
    middle_management: str = "Middle Management"
    first_line_manager: str = "First Line Manager"
    registered_manager: str = "Registered Manager"
    supervisor: str = "Supervisor"
    social_worker: str = "Social Worker"
    senior_care_worker: str = "Senior Care Worker"
    care_worker: str = "Care Worker"
    community_support_and_outreach: str = "Community Support and Outreach Work"
    employment_support: str = "Employment Support"
    advocacy: str = "Advice Guidance and Advocacy"
    occupational_therapist: str = "Occupational Therapist"
    registered_nurse: str = "Registered Nurse"
    allied_health_professional: str = "Allied Health Professional"
    technician: str = "Technician"
    other_care_role: str = "Other care-providing job role"
    care_related_staff: str = (
        "Managers and staff in care-related but not care-providing roles"
    )
    admin_staff: str = "Administrative or office staff not care-providing"
    ancillary_staff: str = "Ancillary staff not care-providing"
    other_non_care_related_staff: str = "Other non-care-providing job roles"
    activites_worker: str = "Activities worker or co-ordinator"
    safeguarding_officer: str = "Safeguarding and reviewing officer"
    occupational_therapist_assistant: str = "Occupational therapist assistant"
    registered_nursing_associate: str = "Registered Nursing Associate"
    nursing_assistant: str = "Nursing Assistant"
    assessment_officer: str = "Assessment officer"
    care_coordinator: str = "Care co-ordinator"
    care_navigator: str = "Care navigator"
    childrens_roles: str = "Any Childrens/young peoples job role"
    deputy_manager: str = "Deputy manager"
    learning_and_development_lead: str = "Learning and development lead"
    team_leader: str = "Team leader"
