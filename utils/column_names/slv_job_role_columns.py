from dataclasses import dataclass


@dataclass
class SLVJobRoleColumns:
    job_role_label: str = "job_role_label"
    employees: str = "employees"
    starters: str = "starters"
    leavers: str = "leavers"
    vacancies: str = "vacancies"
