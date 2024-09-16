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
    absolute_residual: str = "absolute_residual"
    ascwds_filled_posts: str = "ascwds_filled_posts"
    ascwds_filled_posts_dedup: str = ascwds_filled_posts + "_deduplicated"
    ascwds_filled_posts_dedup_clean: str = ascwds_filled_posts_dedup + "_clean"
    ascwds_filled_posts_imputed: str = ascwds_filled_posts + "_imputed"
    ascwds_filled_posts_source: str = ascwds_filled_posts + "_source"
    ascwds_filtering_rule: str = "ascwds_filtering_rule"
    ascwds_workplace_import_date: str = AWPClean.ascwds_workplace_import_date
    average_absolute_residual: str = "average_absolute_residual"
    average_percentage_residual: str = "average_percentage_residual"
    avg_filled_posts_per_bed_ratio: str = "avg_filled_posts_per_bed_ratio"
    avg_residuals_ascwds_filled_posts_dedup_clean_non_res_pir: str = (
        "avg_residuals_ascwds_filled_posts_dedup_clean_non_res_pir"
    )
    avg_residuals_estimate_filled_posts_non_res_pir: str = (
        "avg_residuals_estimate_filled_posts_non_res_pir"
    )
    care_home: str = CQCLClean.care_home
    care_home_model: str = "care_home_model"
    contemporary_ccg: str = ONSClean.contemporary_ccg
    contemporary_constituancy: str = ONSClean.contemporary_constituancy
    contemporary_cssr: str = ONSClean.contemporary_cssr
    contemporary_icb: str = ONSClean.contemporary_icb
    contemporary_icb_region: str = ONSClean.contemporary_icb_region
    contemporary_imd_score: str = ONSClean.contemporary_imd_score
    contemporary_latitude: str = ONSClean.contemporary_latitude
    contemporary_longitude: str = ONSClean.contemporary_longitude
    contemporary_lsoa11: str = ONSClean.contemporary_lsoa11
    contemporary_lsoa21: str = ONSClean.contemporary_lsoa21
    contemporary_msoa11: str = ONSClean.contemporary_msoa11
    contemporary_msoa21: str = ONSClean.contemporary_msoa21
    contemporary_ons_import_date: str = ONSClean.contemporary_ons_import_date
    contemporary_region: str = ONSClean.contemporary_region
    contemporary_rural_urban_indicator_2011: str = (
        ONSClean.contemporary_rural_urban_ind_11
    )
    contemporary_sub_icb: str = ONSClean.contemporary_sub_icb
    cqc_location_import_date: str = CQCLClean.cqc_location_import_date
    cqc_pir_import_date: str = CQCPIRClean.cqc_pir_import_date
    cqc_sector: str = CQCLClean.cqc_sector
    current_ccg: str = ONSClean.current_ccg
    current_constituancy: str = ONSClean.current_constituancy
    current_cssr: str = ONSClean.current_cssr
    current_icb: str = ONSClean.current_icb
    current_icb_region: str = ONSClean.current_icb_region
    current_imd_score: str = ONSClean.current_imd_score
    current_latitude: str = ONSClean.current_latitude
    current_longitude: str = ONSClean.current_longitude
    current_lsoa11: str = ONSClean.current_lsoa11
    current_lsoa21: str = ONSClean.current_lsoa21
    current_msoa11: str = ONSClean.current_msoa11
    current_msoa21: str = ONSClean.current_msoa21
    current_ons_import_date: str = ONSClean.current_ons_import_date
    current_region: str = ONSClean.current_region
    current_rural_urban_indicator_2011: str = ONSClean.current_rural_urban_ind_11
    current_sub_icb: str = ONSClean.current_sub_icb
    date_diff: str = "date_diff"
    distribution_mean: str = "distribution_mean"
    distribution_standard_deviation: str = "distribution_standard_deviation"
    distribution_kurtosis: str = "distribution_kurtosis"
    distribution_skewness: str = "distribution_skewness"
    dormancy: str = CQCLClean.dormancy
    establishment_id: str = AWPClean.establishment_id
    estimate_filled_posts: str = "estimate_filled_posts"
    estimate_filled_posts_source: str = "estimate_filled_posts_source"
    estimate_source: str = "estimate_source"
    estimate_value: str = "estimate_value"
    expected_filled_posts: str = "expected_filled_posts"
    extrapolation_care_home_model: str = "extrapolation_" + care_home_model
    extrapolation_non_res_with_dormancy_model: str = (
        "extrapolation_non_res_with_dormancy_model"
    )
    extrapolation_ratio: str = "extrapolation_ratio"
    extrapolation_rolling_average_model: str = "extrapolation_rolling_average_model"
    features: str = "features"
    filled_posts_per_bed_ratio: str = "filled_posts_per_bed_ratio"
    filled_posts_per_bed_ratio_within_std_resids: str = (
        "filled_posts_per_bed_ratio_within_std_resids"
    )
    first_filled_posts: str = "first_filled_posts"
    first_rolling_average: str = "first_rolling_average"
    first_submission_time: str = "first_submission_time"
    gac_service_types: str = CQCLClean.gac_service_types
    import_month_index: str = "import_month_index"
    imputed_registration_date: str = CQCLClean.imputed_registration_date
    interpolation_model: str = "interpolation_model"
    interpolation_model_ascwds_filled_posts_dedup_clean: str = (
        "interpolation_model_ascwds_filled_posts_dedup_clean"
    )
    interpolation_model_filled_posts_per_bed_ratio: str = (
        "interpolation_model_filled_posts_per_bed_ratio"
    )
    last_filled_posts: str = "last_filled_posts"
    last_rolling_average: str = "last_rolling_average"
    last_submission_time: str = "last_submission_time"
    location_id: str = CQCLClean.location_id
    locations_at_provider_count: str = "locations_at_provider_count"
    locations_in_ascwds_at_provider_count: str = "locations_in_ascwds_at_provider_count"
    locations_in_ascwds_with_data_at_provider_count: str = (
        "locations_in_ascwds_with_data_at_provider_count"
    )
    lower_percentile: str = "lower_percentile"
    max_filled_posts: str = "max_filled_posts"
    max_filled_posts_per_bed_ratio: str = "max_filled_posts_per_bed_ratio"
    max_residual: str = "max_residual"
    min_filled_posts_per_bed_ratio: str = "min_filled_posts_per_bed_ratio"
    min_residual: str = "min_residual"
    model_name: str = "model_name"
    model_run_timestamp: str = "model_run_timestamp"
    model_version: str = "model_version"
    name: str = CQCLClean.name
    next_value: str = "next_value"
    next_value_unix_time: str = "next_value_unix_time"
    non_res_model: str = "non_res_with_pir_model"
    non_res_with_dormancy_model: str = "non_res_with_dormancy_model"
    non_res_without_dormancy_model: str = "non_res_without_dormancy_model"
    number_of_beds: str = CQCLClean.number_of_beds
    number_of_beds_at_provider: str = CQCLClean.number_of_beds + "at_provider"
    number_of_beds_banded: str = "number_of_beds_banded"
    organisation_id: str = AWPClean.organisation_id
    percentage_of_residuals_within_absolute_value: str = (
        "percentage_of_residuals_within_absolute_value"
    )
    percentage_of_residuals_within_percentage_value: str = (
        "percentage_of_residuals_within_percentage_value"
    )
    percentage_of_standardised_residuals_within_limit: str = (
        "percentage_of_standardised_residuals_within_limit"
    )
    percentage_residual: str = "percentage_residual"
    people_directly_employed: str = CQCPIRClean.people_directly_employed
    people_directly_employed_dedup: str = (
        CQCPIRClean.people_directly_employed + "_deduplicated"
    )
    potential_grouped_provider: str = "potential_grouped_provider"
    prediction: str = "prediction"
    previous_value: str = "previous_value"
    previous_value_unix_time: str = "previous_value_unix_time"
    primary_service_type: str = CQCLClean.primary_service_type
    provider_id: str = CQCLClean.provider_id
    provider_name: str = CQCLClean.provider_name
    r2: str = "r2"
    registration_date: str = CQCLClean.registration_date
    registration_status: str = CQCLClean.registration_status
    regulated_activities: str = CQCLClean.regulated_activities
    residual: str = "residual"
    residuals_ascwds_filled_posts_clean_dedup_non_res_pir: str = (
        "residuals_ascwds_filled_posts_clean_dedup_non_res_pir"
    )
    residuals_estimate_filled_posts_non_res_pir: str = (
        "residuals_estimate_filled_posts_non_res_pir"
    )
    rolling_average_model_filled_posts_per_bed_ratio: str = (
        "rolling_average_model_filled_posts_per_bed_ratio"
    )
    rolling_average_model: str = "rolling_average_model"
    rolling_count: str = "rolling_count"
    rolling_sum: str = "rolling_sum"
    service_count: str = "service_count"
    services_offered: str = CQCLClean.services_offered
    specialisms: str = CQCLClean.specialisms
    standardised_residual: str = "standardised_residual"
    time_registered: str = "time_registered"
    total_staff_bounded: str = AWPClean.total_staff_bounded
    unix_time: str = "unix_time"
    upper_percentile: str = "upper_percentile"
    value_unix_time: str = "value_unix_time"
    worker_records_bounded: str = AWPClean.worker_records_bounded
