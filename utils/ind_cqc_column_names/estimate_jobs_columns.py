from dataclasses import dataclass

from utils.ind_cqc_column_names.prepare_locations_cleaned_columns import (
    PrepareLocationsCleanedColumns,
)

@dataclass
class EstimateJobsColumns:
    care_home_model: str = "care_home_model"
    cqc_sector = PrepareLocationsCleanedColumns.cqc_sector
    estimate_job_count: str = "estimate_job_count"
    estimate_job_count_source: str = "estimate_job_count_source"
    extrapolation_model: str = "extrapolation_model"
    interpolation_model: str = "interpolation_model"
    job_count = PrepareLocationsCleanedColumns.job_count
    job_count_unfiltered = PrepareLocationsCleanedColumns.job_count_unfiltered
    job_count_unfiltered_source = PrepareLocationsCleanedColumns.job_count_unfiltered_source
    local_authority = PrepareLocationsCleanedColumns.local_authority
    location_id = PrepareLocationsCleanedColumns.location_id
    non_res_with_pir_model: str = "non_res_with_pir_model"
    number_of_beds = PrepareLocationsCleanedColumns.number_of_beds
    people_directly_employed = PrepareLocationsCleanedColumns.people_directly_employed
    primary_service_type = PrepareLocationsCleanedColumns.primary_service_type
    rolling_average_model: str = "rolling_average_model"
    run_day = PrepareLocationsCleanedColumns.run_day
    run_month = PrepareLocationsCleanedColumns.run_month
    run_year = PrepareLocationsCleanedColumns.run_year
    services_offered = PrepareLocationsCleanedColumns.services_offered
    unix_time: str = "unix_time"
    version = PrepareLocationsCleanedColumns.version

