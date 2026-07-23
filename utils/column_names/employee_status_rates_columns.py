from dataclasses import dataclass


@dataclass
class EmployeeStatusRatesColumns:
    service: str = "service"
    weighting_year: str = "weighting_year"
    weighting_job_role: str = "weighting_job_role"
    permanent: str = "permanent"
    temporary: str = "temporary"
    bank_or_pool: str = "bank_or_pool"
    agency: str = "agency"
    other: str = "other"
    filled_posts: str = "filled_posts"
    weighting_date: str = "weighting_date"
    emp_stat_perm: str = "emp_stat_perm"
    emp_stat_temp: str = "emp_stat_temp"
    emp_stat_bank_or_pool: str = "emp_stat_bank_or_pool"
    emp_stat_agency: str = "emp_stat_agency"
    emp_stat_other: str = "emp_stat_other"
