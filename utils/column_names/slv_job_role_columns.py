from dataclasses import dataclass


@dataclass
class SLVJobRoleColumns:
    published_job_role_label: str = "published_job_role_label"
    employees: str = "employees"
    starters: str = "starters"
    leavers: str = "leavers"
    vacancies: str = "vacancies"
    turnover_rate: str = "turnover_rate"
    starter_rate: str = "starter_rate"
    vacancy_rate: str = "vacancy_rate"
    estimated_emp_stat_perm: str = "estimated_emp_stat_perm"
    estimated_emp_stat_temp: str = "estimated_emp_stat_temp"
    estimated_emp_stat_bank_or_pool: str = "estimated_emp_stat_bank_or_pool"
    estimated_emp_stat_agency: str = "estimated_emp_stat_agency"
    estimated_emp_stat_other: str = "estimated_emp_stat_other"
    estimated_employees: str = "estimated_employees"
