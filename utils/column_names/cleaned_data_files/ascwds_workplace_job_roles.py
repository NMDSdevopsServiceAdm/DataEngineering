from dataclasses import dataclass


@dataclass
class AscwdsWorkplaceJobRolesColumns:
    job_role_code: str = "job_role_code"
    employees: str = "employees"
    starters: str = "starters"
    leavers: str = "leavers"
    vacancies: str = "vacancies"
