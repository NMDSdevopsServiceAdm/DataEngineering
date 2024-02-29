from dataclasses import dataclass


@dataclass
class PartitionKeys:
    day: str = "day"
    import_date: str = "import_date"
    month: str = "month"
    year: str = "year"


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
    lower_super_output_area: str = "lsoa"
    middle_super_output_area: str = "msoa"
    nhs_england_region: str = "nhs_england_region"
    number_of_beds: str = "number_of_beds"
    ons_import_date: str = "ons_import_date"
    ons_region: str = "ons_region"
    organisation_type: str = "organisation_type"
    orgid: str = "orgid"
    local_or_unitary_authority: str = "oslaua"
    people_directly_employed: str = "people_directly_employed"
    postcode: str = "postal_code"
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
    sustainability_and_transformation_partnership: str = "stp"
    version: str = "version"


@dataclass
class PrepareLocationsCleanedColumns:
    care_home = PrepareLocationsColumns.care_home
    cqc_sector = PrepareLocationsColumns.cqc_sector
    job_count: str = "job_count"
    job_count_unfiltered = PrepareLocationsColumns.job_count_unfiltered
    job_count_unfiltered_source = PrepareLocationsColumns.job_count_unfiltered_source
    local_authority = PrepareLocationsColumns.local_authority
    location_id = PrepareLocationsColumns.location_id
    number_of_beds = PrepareLocationsColumns.number_of_beds
    ons_region = PrepareLocationsColumns.ons_region
    people_directly_employed = PrepareLocationsColumns.people_directly_employed
    primary_service_type = PrepareLocationsColumns.primary_service_type
    registration_status = PrepareLocationsColumns.registration_status
    rural_urban_indicator_2011: str = "rui_2011"
    run_day: str = "run_day"
    run_month: str = "run_month"
    run_year: str = "run_year"
    services_offered = PrepareLocationsColumns.services_offered
    snapshot_date = PrepareLocationsColumns.snapshot_date
    snapshot_day = PrepareLocationsColumns.snapshot_day
    snapshot_month = PrepareLocationsColumns.snapshot_month
    snapshot_year = PrepareLocationsColumns.snapshot_year
    version = PrepareLocationsColumns.version


@dataclass
class FeatureEngineeringColumns:
    care_home = PrepareLocationsCleanedColumns.care_home
    features: str = "features"
    job_count = PrepareLocationsCleanedColumns.job_count
    location_id = PrepareLocationsCleanedColumns.location_id
    number_of_beds = PrepareLocationsCleanedColumns.number_of_beds
    ons_region = PrepareLocationsCleanedColumns.ons_region
    partition_0: str = "partition_0"
    people_directly_employed = PrepareLocationsCleanedColumns.people_directly_employed
    snapshot_date = PrepareLocationsCleanedColumns.snapshot_date
    snapshot_day = PrepareLocationsCleanedColumns.snapshot_day
    snapshot_month = PrepareLocationsCleanedColumns.snapshot_month
    snapshot_year = PrepareLocationsCleanedColumns.snapshot_year


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
    job_count_unfiltered_source = (
        PrepareLocationsCleanedColumns.job_count_unfiltered_source
    )
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
