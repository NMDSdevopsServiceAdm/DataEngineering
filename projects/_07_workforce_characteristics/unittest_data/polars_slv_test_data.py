EMPLOYEE_STATUS_RATES_CSV_HEADER = (
    "service,weighting_year,weighting_job_role,permanent,temporary,bank_or_pool,"
    "agency,other,filled_posts,weighting_date,emp_stat_perm,emp_stat_temp,"
    "emp_stat_bank_or_pool,emp_stat_agency,emp_stat_other"
)

EMPLOYEE_STATUS_RATES_TARGET_YEAR_ROW = (
    "Care home,2025/26,Care worker,120,30,10,5,5,170,2025-01-01,"
    "0.65,0.18,0.08,0.06,0.03"
)

EMPLOYEE_STATUS_RATES_OTHER_YEAR_ROW = (
    "Nursing home,2024/25,Registered nurse,90,20,6,3,1,120,2024-01-01,"
    "0.75,0.1,0.08,0.05,0.02"
)

EMPLOYEE_STATUS_RATES_BLANK_ROW = ",,,,,,,,,,,,,,"

employee_status_rates_expected_rows = [
    ("Care home", "Care worker", 0.65, 0.18, 0.08, 0.06, 0.03),
]
