from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as CQCPIRClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)


@dataclass
class PartitionKeys:
    day: str = "day"
    import_date: str = "import_date"
    month: str = "month"
    year: str = "year"


@dataclass
class IndCqcColumns:
    cqc_location_import_date: str = CQCLClean.cqc_location_import_date
    location_id: str = CQCLClean.location_id
    name: str = CQCLClean.name
    provider_id: str = CQCLClean.provider_id
    provider_name: str = CQCLClean.provider_name
    cqc_sector: str = CQCLClean.cqc_sector
    registration_status: str = CQCLClean.registration_status
    registration_date: str = CQCLClean.registration_date
    imputed_registration_date: str = CQCLClean.imputed_registration_date
    dormancy: str = CQCLClean.dormancy
    care_home: str = CQCLClean.care_home
    number_of_beds: str = CQCLClean.number_of_beds
    regulated_activities: str = CQCLClean.regulated_activities
    gac_service_types: str = CQCLClean.gac_service_types
    services_offered: str = CQCLClean.services_offered
    specialisms: str = CQCLClean.specialisms
    primary_service_type: str = CQCLClean.primary_service_type
    ascwds_workplace_import_date: str = AWPClean.ascwds_workplace_import_date
    establishment_id: str = AWPClean.establishment_id
    organisation_id: str = AWPClean.organisation_id
    total_staff_bounded: str = AWPClean.total_staff_bounded
    worker_records_bounded: str = AWPClean.worker_records_bounded
    cqc_pir_import_date: str = CQCPIRClean.cqc_pir_import_date
    people_directly_employed: str = CQCPIRClean.people_directly_employed
    people_directly_employed_dedup: str = (
        CQCPIRClean.people_directly_employed + "_deduplicated"
    )
    contemporary_ons_import_date: str = ONSClean.contemporary_ons_import_date
    contemporary_cssr: str = ONSClean.contemporary_cssr
    contemporary_region: str = ONSClean.contemporary_region
    contemporary_sub_icb: str = ONSClean.contemporary_sub_icb
    contemporary_icb: str = ONSClean.contemporary_icb
    contemporary_icb_region: str = ONSClean.contemporary_icb_region
    contemporary_ccg: str = ONSClean.contemporary_ccg
    contemporary_latitude: str = ONSClean.contemporary_latitude
    contemporary_longitude: str = ONSClean.contemporary_longitude
    contemporary_imd_score: str = ONSClean.contemporary_imd_score
    contemporary_lsoa11: str = ONSClean.contemporary_lsoa11
    contemporary_msoa11: str = ONSClean.contemporary_msoa11
    contemporary_rural_urban_indicator_2011: str = (
        ONSClean.contemporary_rural_urban_ind_11
    )
    contemporary_lsoa21: str = ONSClean.contemporary_lsoa21
    contemporary_msoa21: str = ONSClean.contemporary_msoa21
    contemporary_constituancy: str = ONSClean.contemporary_constituancy
    current_ons_import_date: str = ONSClean.current_ons_import_date
    current_cssr: str = ONSClean.current_cssr
    current_region: str = ONSClean.current_region
    current_sub_icb: str = ONSClean.current_sub_icb
    current_icb: str = ONSClean.current_icb
    current_icb_region: str = ONSClean.current_icb_region
    current_ccg: str = ONSClean.current_ccg
    current_latitude: str = ONSClean.current_latitude
    current_longitude: str = ONSClean.current_longitude
    current_imd_score: str = ONSClean.current_imd_score
    current_lsoa11: str = ONSClean.current_lsoa11
    current_msoa11: str = ONSClean.current_msoa11
    current_rural_urban_indicator_2011: str = ONSClean.current_rural_urban_ind_11
    current_lsoa21: str = ONSClean.current_lsoa21
    current_msoa21: str = ONSClean.current_msoa21
    current_constituancy: str = ONSClean.current_constituancy
    ascwds_filled_posts: str = "ascwds_filled_posts"
    ascwds_filled_posts_source: str = ascwds_filled_posts + "_source"
    ascwds_filled_posts_clean: str = ascwds_filled_posts + "_clean"
    ascwds_filled_posts_dedup_clean: str = ascwds_filled_posts + "_clean_deduplicated"
    service_count: str = "service_count"
    date_diff: str = "date_diff"
    features: str = "features"
    estimate_filled_posts: str = "estimate_filled_posts"
    estimate_filled_posts_source: str = "estimate_filled_posts_source"
    r2: str = "r2"
    model_name: str = "model_name"
    model_version: str = "model_version"
    model_run_timestamp: str = "model_run_timestamp"
    unix_time: str = "unix_time"
    include_in_count_of_filled_posts: str = "include_in_count_of_filled_posts"
    rolling_count_of_filled_posts: str = "rolling_count_of_filled_posts"
    rolling_sum_of_filled_posts: str = "rolling_sum_of_filled_posts"
    rolling_average_model: str = "rolling_average_model"
    max_filled_posts: str = "max_filled_posts"
    first_submission_time: str = "first_submission_time"
    first_rolling_average: str = "first_rolling_average"
    first_filled_posts: str = "first_filled_posts"
    last_submission_time: str = "last_submission_time"
    last_rolling_average: str = "last_rolling_average"
    last_filled_posts: str = "last_filled_posts"
    extrapolation_ratio: str = "extrapolation_ratio"
    prediction: str = "prediction"
    care_home_model: str = "care_home_model"
    extrapolation_care_home_model: str = "extrapolation_" + care_home_model
    previous_filled_posts: str = "previous_filled_posts"
    next_filled_posts: str = "next_filled_posts"
    filled_posts_unix_time: str = "filled_posts_unix_time"
    previous_filled_posts_unix_time: str = "previous_filled_posts_unix_time"
    next_filled_posts_unix_time: str = "next_filled_posts_unix_time"
    interpolation_model: str = "interpolation_model"
    rolling_average: str = "rolling_average"
    non_res_model: str = "non_res_with_pir_model"
    residuals_estimate_filled_posts_non_res_pir: str = (
        "residuals_estimate_filled_posts_non_res_pir"
    )
    residuals_ascwds_filled_posts_clean_dedup_non_res_pir: str = (
        "residuals_ascwds_filled_posts_clean_dedup_non_res_pir"
    )


# DONT IMPORT FROM BELOW THIS LINE
# WE'LL BE REMOVING THESE WHEN PREPARE LOCATIONS SCRIPT GOES
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
    filled_posts_unfiltered: str = "filled_posts_unfiltered"
    filled_posts_unfiltered_source: str = "filled_posts_unfiltered_source"
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
    filled_posts: str = "filled_posts"
    filled_posts_unfiltered = PrepareLocationsColumns.filled_posts_unfiltered
    filled_posts_unfiltered_source = (
        PrepareLocationsColumns.filled_posts_unfiltered_source
    )
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
    filled_posts = PrepareLocationsCleanedColumns.filled_posts
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
    estimate_filled_posts: str = "estimate_filled_posts"
    estimate_filled_posts_source: str = "estimate_filled_posts_source"
    extrapolation_model: str = "extrapolation_model"
    interpolation_model: str = "interpolation_model"
    filled_posts = PrepareLocationsCleanedColumns.filled_posts
    filled_posts_unfiltered = PrepareLocationsCleanedColumns.filled_posts_unfiltered
    filled_posts_unfiltered_source = (
        PrepareLocationsCleanedColumns.filled_posts_unfiltered_source
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
