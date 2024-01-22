from dataclasses import dataclass


@dataclass
class EstimateJobsColumns:
    care_home_model: str = "care_home_model"
    cqc_sector: str = "cqc_sector"
    estimate_job_count: str = "estimate_job_count"
    estimate_job_count_source: str = "estimate_job_count_source"
    extrapolation_model: str = "extrapolation_model"
    interpolation_model: str = "interpolation_model"
    job_count: str = "job_count"
    job_count_unfiltered: str = "job_count_unfiltered"
    job_count_unfiltered_source: str = "job_count_unfiltered_source"
    local_authority: str = "local_authority"
    locationid: str = "locationid"
    non_res_with_pir_model: str = "non_res_with_pir_model"
    number_of_beds: str = "number_of_beds"
    people_directly_employed: str = "people_directly_employed"
    primary_service_type: str = "primary_service_type"
    rolling_average_model: str = "rolling_average_model"
    run_day: str = "run_day"
    run_month: str = "run_month"
    run_year: str = "run_year"
    services_offered: str = "services_offered"
    snapshot_date: str = "snapshot_date"
    unix_time: str = "unix_time"
    version: str = "version"

