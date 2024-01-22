from dataclasses import dataclass

from utils.ind_cqc_column_names.prepare_locations_columns import (
    PrepareLocationsColumns,
)


@dataclass
class PrepareLocationsCleanedColumns:
    care_home = PrepareLocationsColumns.care_home
    cqc_sector = PrepareLocationsColumns.cqc_sector
    job_count: str = "job_count"
    job_count_unfiltered = PrepareLocationsColumns.job_count_unfiltered
    job_count_unfiltered_source = PrepareLocationsColumns.job_count_unfiltered_source
    local_authority = PrepareLocationsColumns.local_authority
    location_id = PrepareLocationsColumns.location_id
    number_of_beds =PrepareLocationsColumns.number_of_beds
    ons_region = PrepareLocationsColumns.ons_region
    people_directly_employed = PrepareLocationsColumns.people_directly_employed
    primary_service_type = PrepareLocationsColumns.primary_service_type
    registration_status =PrepareLocationsColumns.registration_status
    rui_2011: str = "rui_2011"
    run_day: str = "run_day"
    run_month: str = "run_month"
    run_year: str = "run_year"
    services_offered = PrepareLocationsColumns.services_offered
    snapshot_date = PrepareLocationsColumns.snapshot_date
    snapshot_day = PrepareLocationsColumns.snapshot_day
    snapshot_month = PrepareLocationsColumns.snapshot_month
    snapshot_year = PrepareLocationsColumns.snapshot_year
    version = PrepareLocationsColumns.version
