from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


@dataclass
class AscwdsWorkerValueLabelsMainjrid:
    """The possible values of the mainjrid column in ascwds worker data"""

    column_name: str = AWK.main_job_role_id

    labels_dict = {
        "-1": "Not known",
        "1": "Senior Management",
        "2": "Middle Management",
        "3": "First Line Manager",
        "4": "Registered Manager",
        "5": "Supervisor",
        "6": "Social Worker",
        "7": "Senior Care Worker",
        "8": "Care Worker",
        "9": "Community Support and Outreach Work",
        "10": "Employment Support",
        "11": "Advice Guidance and Advocacy",
        "15": "Occupational Therapist",
        "16": "Registered Nurse",
        "17": "Allied Health Professional",
        "22": "Technician",
        "23": "Other care-providing job role",
        "24": "Managers and staff in care-related but not care-providing roles",
        "25": "Administrative or office staff not care-providing",
        "26": "Ancillary staff not care-providing",
        "27": "Other non-care-providing job roles",
        "34": "Activities worker or co-ordinator",
        "35": "Safeguarding and reviewing officer",
        "36": "Occupational therapist assistant",
        "37": "Registered Nursing Associate",
        "38": "Nursing Assistant",
        "39": "Assessment officer",
        "40": "Care co-ordinator",
        "41": "Care navigator",
        "42": "Any Childrens/young peoples job role",
        "43": "Deputy manager",
        "44": "Learning and development lead",
        "45": "Team leader",
    }
