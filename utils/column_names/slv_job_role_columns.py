from dataclasses import dataclass


@dataclass
class SlvJobRoleColumns:
    employees: str = "employees"
    job_role_code: str = "job_role_code"
    leavers: str = "leavers"
    starters: str = "starters"
    vacancies: str = "vacancies"
