from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)
from utils.column_values.ascwds_worker_values import (
    MainJobRole,
)


@dataclass
class AscwdsWorkerValueLabelsMainjrid:
    """The possible values of the mainjrid column in ascwds worker data"""

    column_name: str = AWK.main_job_role_id

    labels_dict = {
        "-1": MainJobRole.not_known,
        "1": MainJobRole.senior_management,
        "2": MainJobRole.middle_management,
        "3": MainJobRole.first_line_manager,
        "4": MainJobRole.registered_manager,
        "5": MainJobRole.supervisor,
        "6": MainJobRole.social_worker,
        "7": MainJobRole.senior_care_worker,
        "8": MainJobRole.care_worker,
        "9": MainJobRole.community_support_and_outreach,
        "10": MainJobRole.employment_support,
        "11": MainJobRole.advocacy,
        "15": MainJobRole.occupational_therapist,
        "16": MainJobRole.registered_nurse,
        "17": MainJobRole.allied_health_professional,
        "22": MainJobRole.technician,
        "23": MainJobRole.other_care_role,
        "24": MainJobRole.care_related_staff,
        "25": MainJobRole.admin_staff,
        "26": MainJobRole.ancillary_staff,
        "27": MainJobRole.other_non_care_related_staff,
        "34": MainJobRole.activites_worker,
        "35": MainJobRole.safeguarding_officer,
        "36": MainJobRole.occupational_therapist_assistant,
        "37": MainJobRole.registered_nursing_associate,
        "38": MainJobRole.nursing_assistant,
        "39": MainJobRole.assessment_officer,
        "40": MainJobRole.care_coordinator,
        "41": MainJobRole.care_navigator,
        "42": MainJobRole.childrens_roles,
        "43": MainJobRole.deputy_manager,
        "44": MainJobRole.learning_and_development_lead,
        "45": MainJobRole.team_leader,
    }
