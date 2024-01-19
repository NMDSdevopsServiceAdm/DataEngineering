from dataclasses import dataclass


@dataclass
class EstimateJobsColumns:
    locationid: str = "locationid"
    unix_time: str = "unix_time"
    snapshot_date: str = "snapshot_date"
    primary_service_type: str = "primary_service_type"
    services_offered: str = "services_offered"
    people_directly_employed: str = "people_directly_employed"
    number_of_beds: str = "number_of_beds"
    job_count_unfiltered: str = "job_count_unfiltered"
    job_count_unfiltered_source: str = "job_count_unfiltered_source"
    job_count: str = "job_count"
    local_authority: str = "local_authority"
    cqc_sector: str = "cqc_sector"
    estimate_job_count: str = "estimate_job_count"
    estimate_job_count_source: str = "estimate_job_count_source"
    rolling_average_model: str = "rolling_average_model"
    extrapolation_model: str = "extrapolation_model"
    care_home_model: str = "care_home_model"
    interpolation_model: str = "interpolation_model"
    non_res_with_pir_model: str = "non_res_with_pir_model"
    version: str = "version"
    run_year: str = "run_year"
    run_month: str = "run_month"
    run_day: str = "run_day"
