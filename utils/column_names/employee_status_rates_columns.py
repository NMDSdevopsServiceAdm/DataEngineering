from dataclasses import dataclass


@dataclass
class EmployeeStatusRatesColumns:
    service: str = "service"
    weighting_job_role: str = "weighting_job_role"
    emp_stat_perm: str = "emp_stat_perm"
    emp_stat_temp: str = "emp_stat_temp"
    emp_stat_bank_or_pool: str = "emp_stat_bank_or_pool"
    emp_stat_agency: str = "emp_stat_agency"
    emp_stat_other: str = "emp_stat_other"
