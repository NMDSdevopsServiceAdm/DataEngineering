from dataclasses import dataclass


@dataclass
class PrepareLocationsCleanedColumns:
    carehome: str = "carehome"
    cqc_sector: str = "cqc_sector"
    job_count: str = "job_count"
    job_count_unfiltered: str = "job_count_unfiltered"
    job_count_unfiltered_source: str = "job_count_unfiltered_source"
    local_authority: str = "local_authority"
    locationid: str = "locationid"
    number_of_beds: str = "number_of_beds"
    ons_region: str = "ons_region"
    people_directly_employed: str = "people_directly_employed"
    primary_service_type: str = "primary_service_type"
    registration_status: str = "registration_status"
    rui_2011: str = "rui_2011"
    run_day: str = "run_day"
    run_month: str = "run_month"
    run_year: str = "run_year"
    services_offered: str = "services_offered"
    snapshot_date: str = "snapshot_date"
    snapshot_day: str = "snapshot_day"
    snapshot_month: str = "snapshot_month"
    snapshot_year: str = "snapshot_year"
    version: str = "version"
