from dataclasses import dataclass


@dataclass
class SLVJobRoleColumns:
    published_job_role_label: str = "published_job_role_label"
    employees: str = "employees"
    starters: str = "starters"
    leavers: str = "leavers"
    vacancies: str = "vacancies"
