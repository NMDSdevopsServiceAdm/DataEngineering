import polars as pl

employee_status_rates_schema = {
    "service": pl.Categorical(),
    "weighting_year": pl.Categorical(),
    "weighting_job_role": pl.Categorical(),
    "emp_stat_perm": pl.Float32,
    "emp_stat_temp": pl.Float32,
    "emp_stat_bank_or_pool": pl.Float32,
    "emp_stat_agency": pl.Float32,
    "emp_stat_other": pl.Float32,
}

employee_status_rates_expected_schema = {
    "service": pl.Categorical(),
    "weighting_job_role": pl.Categorical(),
    "emp_stat_perm": pl.Float32,
    "emp_stat_temp": pl.Float32,
    "emp_stat_bank_or_pool": pl.Float32,
    "emp_stat_agency": pl.Float32,
    "emp_stat_other": pl.Float32,
}
