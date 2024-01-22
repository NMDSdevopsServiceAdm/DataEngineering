from dataclasses import dataclass


@dataclass
class PrepareLocationsColumns:
    ascwds_workplace_import_date: str = "ascwds_workplace_import_date"
    care_home: str = "carehome"
    clinical_commisioning_group: str = "clinical_commisioning_group"
    constituency: str = "constituency"
    country: str = "country"
    cqc_coverage_in_ascwds: str = "cqc_coverage_in_ascwds"
    cqc_locations_import_date: str = "cqc_locations_import_date"
    cqc_pir_import_date: str = "cqc_pir_import_date"
    cqc_providers_import_date: str = "cqc_providers_import_date"
    cqc_sector: str = "cqc_sector"
    deregistration_date: str = "deregistration_date"
    dormancy: str = "dormancy"
    establishmentid: str = "establishmentid"
    job_count_unfiltered: str = "job_count_unfiltered"
    job_count_unfiltered_source: str = "job_count_unfiltered_source"
    local_authority: str = "local_authority"
    location_name: str = "location_name"
    location_type: str = "location_type"
    location_id: str = "locationid"
    lsoa: str = "lsoa"
    msoa: str = "msoa"
    nhs_england_region: str = "nhs_england_region"
    number_of_beds: str = "number_of_beds"
    ons_import_date: str = "ons_import_date"
    ons_region: str = "ons_region"
    organisation_type: str = "organisation_type"
    orgid: str = "orgid"
    oslaua: str = "oslaua"
    people_directly_employed: str = "people_directly_employed"
    postal_code: str = "postal_code"
    primary_service_type: str = "primary_service_type"
    provider_name: str = "provider_name"
    providerid: str = "providerid"
    region: str = "region"
    registration_date: str = "registration_date"
    registration_status: str = "registration_status"
    rural_urban_indicator: str = "rural_urban_indicator"
    services_offered: str = "services_offered"
    snapshot_date: str = "snapshot_date"
    snapshot_day: str = "snapshot_day"
    snapshot_month: str = "snapshot_month"
    snapshot_year: str = "snapshot_year"
    stp: str = "stp"
    version: str = "version"
