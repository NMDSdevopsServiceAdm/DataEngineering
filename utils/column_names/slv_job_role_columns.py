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
    starters_cleaned: str = starters + "_cleaned"
    leavers_cleaned: str = leavers + "_cleaned"
    vacancies_cleaned: str = vacancies + "_cleaned"
    starters_filtering_rule: str = starters + "_filtering_rule"
    leavers_filtering_rule: str = leavers + "_filtering_rule"
    vacancies_filtering_rule: str = vacancies + "_filtering_rule"
    starters_cleaned_dedup: str = starters_cleaned + "_deduplicated"
    leavers_cleaned_dedup: str = leavers_cleaned + "_deduplicated"
    vacancies_cleaned_dedup: str = vacancies_cleaned + "_deduplicated"
    turnover_rate_dedup: str = turnover_rate + "_deduplicated"
    starter_rate_dedup: str = starter_rate + "_deduplicated"
    vacancy_rate_dedup: str = vacancy_rate + "_deduplicated"
    estimated_emp_stat_perm: str = "estimated_emp_stat_perm"
    estimated_emp_stat_temp: str = "estimated_emp_stat_temp"
    estimated_emp_stat_bank_or_pool: str = "estimated_emp_stat_bank_or_pool"
    estimated_emp_stat_agency: str = "estimated_emp_stat_agency"
    estimated_emp_stat_other: str = "estimated_emp_stat_other"
    estimated_employees: str = "estimated_employees"


@dataclass
class SLVEmploymentStatusColumns:
    employment_status_count: str = "emplstat_count"
    permanent_count: str = "emplstat_permanent_count"
    temporary_count: str = "emplstat_temporary_count"
    bank_or_pool_count: str = "emplstat_bank_or_pool_count"
    agency_count: str = "emplstat_agency_count"
    other_count: str = "emplstat_other_count"
