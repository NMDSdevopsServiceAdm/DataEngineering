from dataclasses import dataclass


@dataclass
class PrepareLocationsCleanedColumns:
    locationid: str = "locationid"
    snapshot_date: str = "snapshot_date"
    snapshot_day: str = "snapshot_day"
    snapshot_month: str = "snapshot_month"
    snapshot_year: str = "snapshot_year"
    local_authority: str = "local_authority"
    ons_region: str = "ons_region"
    rui_2011: str = "rui_2011"
    services_offered: str = "services_offered"
    carehome: str = "carehome"
    primary_service_type: str = "primary_service_type"
    cqc_sector: str = "cqc_sector"
    registration_status: str = "registration_status"
    number_of_beds: str = "number_of_beds"
    people_directly_employed: str = "people_directly_employed"
    job_count_unfiltered_source: str = "job_count_unfiltered_source"
    job_count_unfiltered: str = "job_count_unfiltered"
    job_count: str = "job_count"
    version: str = "version"
    run_year: str = "run_year"
    run_month: str = "run_month"
    run_day: str = "run_day"
