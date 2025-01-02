from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
)


@dataclass
class AscwdsWorkerValueLabelsMainjrid:
    """The possible values of the mainjrid column in ascwds worker data"""

    column_name: str = AWKClean.main_job_role_clean

    labels_dict = {
        "-1": MainJobRoleLabels.not_known,
        "1": MainJobRoleLabels.senior_management,
        "2": MainJobRoleLabels.middle_management,
        "3": MainJobRoleLabels.first_line_manager,
        "4": MainJobRoleLabels.registered_manager,
        "5": MainJobRoleLabels.supervisor,
        "6": MainJobRoleLabels.social_worker,
        "7": MainJobRoleLabels.senior_care_worker,
        "8": MainJobRoleLabels.care_worker,
        "9": MainJobRoleLabels.community_support_and_outreach,
        "10": MainJobRoleLabels.employment_support,
        "11": MainJobRoleLabels.advocacy,
        "15": MainJobRoleLabels.occupational_therapist,
        "16": MainJobRoleLabels.registered_nurse,
        "17": MainJobRoleLabels.allied_health_professional,
        "22": MainJobRoleLabels.technician,
        "23": MainJobRoleLabels.other_care_role,
        "24": MainJobRoleLabels.care_related_staff,
        "25": MainJobRoleLabels.admin_staff,
        "26": MainJobRoleLabels.ancillary_staff,
        "27": MainJobRoleLabels.other_non_care_related_staff,
        "34": MainJobRoleLabels.activites_worker,
        "35": MainJobRoleLabels.safeguarding_officer,
        "36": MainJobRoleLabels.occupational_therapist_assistant,
        "37": MainJobRoleLabels.registered_nursing_associate,
        "38": MainJobRoleLabels.nursing_assistant,
        "39": MainJobRoleLabels.assessment_officer,
        "40": MainJobRoleLabels.care_coordinator,
        "42": MainJobRoleLabels.childrens_roles,
        "43": MainJobRoleLabels.deputy_manager,
        "44": MainJobRoleLabels.learning_and_development_lead,
        "45": MainJobRoleLabels.team_leader,
        "46": MainJobRoleLabels.data_analyst,
        "47": MainJobRoleLabels.data_governance_manager,
        "48": MainJobRoleLabels.it_and_digital_support,
        "49": MainJobRoleLabels.it_manager,
        "50": MainJobRoleLabels.it_service_desk_manager,
        "51": MainJobRoleLabels.software_developer,
        "52": MainJobRoleLabels.support_worker,
    }
