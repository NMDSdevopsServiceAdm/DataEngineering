from dataclasses import dataclass


@dataclass
class SLVJobRoleColumns:
    job_role_label: str = "job_role_label"
    employees: str = "employees"
    starters: str = "starters"
    leavers: str = "leavers"
    vacancies: str = "vacancies"
    filled_posts_perm: str = "filled_posts_perm"
    filled_posts_temp: str = "filled_posts_temp"
    filled_posts_bank_or_pool: str = "filled_posts_bank_or_pool"
    filled_posts_agency: str = "filled_posts_agency"
    filled_posts_other: str = "filled_posts_other"
    employment_status_estimate_error: str = "employment_status_estimate_error"
