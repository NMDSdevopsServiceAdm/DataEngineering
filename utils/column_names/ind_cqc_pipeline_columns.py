from dataclasses import dataclass

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
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


class ArchivePartitionKeys:
    archive_day: str = "archive_day"
    archive_month: str = "archive_month"
    archive_year: str = "archive_year"
    archive_timestamp: str = "archive_timestamp"


@dataclass
class IndCqcColumns:
    absolute_residual: str = "absolute_residual"
    activity_count: str = "activity_count"
    ascwds_pir_merged: str = "ascwds_pir_merged"
    ascwds_filled_posts: str = "ascwds_filled_posts"
    ascwds_filled_posts_dedup: str = ascwds_filled_posts + "_deduplicated"
    ascwds_filled_posts_dedup_clean: str = ascwds_filled_posts_dedup + "_clean"
    ascwds_filled_posts_dedup_clean_repeated: str = (
        ascwds_filled_posts_dedup_clean + "_repeated"
    )
    ascwds_filled_posts_source: str = ascwds_filled_posts + "_source"
    ascwds_filtering_rule: str = "ascwds_filtering_rule"
    ascwds_job_group_counts: str = "ascwds_job_group_counts"
    ascwds_job_group_ratios: str = "ascwds_job_group_ratios"
    ascwds_job_role_counts: str = "ascwds_job_role_counts"
    ascwds_job_role_counts_temporary: str = "ascwds_job_role_counts_temporary"
    ascwds_job_role_counts_exploded: str = "ascwds_job_role_counts_exploded"
    ascwds_job_role_counts_rolling_sum: str = "ascwds_job_role_counts_rolling_sum"
    ascwds_job_role_ratios: str = "ascwds_job_role_ratios"
    ascwds_job_role_ratios_temporary: str = "ascwds_job_role_ratios_temporary"
    ascwds_job_role_ratios_interpolated: str = "ascwds_job_role_ratios_interpolated"
    ascwds_job_role_ratios_exploded: str = "ascwds_job_role_ratios_exploded"
    ascwds_job_role_counts_by_primary_service: str = (
        "ascwds_job_role_counts_by_primary_service"
    )
    ascwds_job_role_ratios_by_primary_service: str = (
        "ascwds_job_role_ratios_by_primary_service"
    )
    ascwds_job_role_ratios_merged: str = "ascwds_job_role_ratios_merged"
    ascwds_job_role_ratios_merged_source: str = "ascwds_job_role_ratios_merged_source"
    ascwds_rate_of_change_trendline_model: str = "rolling_rate_of_change_model"
    ascwds_worker_import_date: str = AWKClean.ascwds_worker_import_date
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
    banded_bed_ratio_rolling_average_model: str = (
        "banded_bed_ratio_rolling_average_model"
    )
    care_home: str = CQCLClean.care_home
    care_home_model: str = "care_home_model"
    code: str = CQCLClean.code
    combined_ratio_and_filled_posts: str = "combined_ratio_and_filled_posts"
    contacts: str = CQCLClean.contacts
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
    current_rural_urban_indicator_2011_for_non_res_model: str = (
        ONSClean.current_rural_urban_ind_11 + "_for_non_res_model"
    )
    current_sub_icb: str = ONSClean.current_sub_icb
    difference_between_estimate_and_cqc_registered_managers: str = (
        "difference_between_estimate_and_cqc_registered_managers"
    )
    distribution_mean: str = "distribution_mean"
    distribution_standard_deviation: str = "distribution_standard_deviation"
    distribution_kurtosis: str = "distribution_kurtosis"
    distribution_skewness: str = "distribution_skewness"
    dormancy: str = CQCLClean.dormancy
    establishment_id: str = AWPClean.establishment_id
    estimate_filled_posts: str = "estimate_filled_posts"
    estimate_filled_posts_source: str = "estimate_filled_posts_source"
    estimate_filled_posts_by_job_role: str = "estimate_filled_posts_by_job_role"
    estimate_source: str = "estimate_source"
    estimate_value: str = "estimate_value"
    expected_filled_posts: str = "expected_filled_posts"
    extrapolation_backwards: str = "extrapolation_backwards"
    extrapolation_forwards: str = "extrapolation_forwards"
    extrapolation_model: str = "extrapolation_model"
    extrapolation_ratio: str = "extrapolation_ratio"
    features: str = "features"
    filled_posts_per_bed_ratio: str = "filled_posts_per_bed_ratio"
    filled_posts_per_bed_ratio_within_std_resids: str = (
        "filled_posts_per_bed_ratio_within_std_resids"
    )
    final_submission_time: str = "final_submission_time"
    first_filled_posts: str = "first_filled_posts"
    first_model_value: str = "first_model_value"
    first_non_null_value: str = "first_non_null_value"
    first_rolling_average: str = "first_rolling_average"
    first_submission_time: str = "first_submission_time"
    gac_service_types: str = CQCLClean.gac_service_types
    has_non_null_value: str = "has_non_null_value"
    imputed_gac_service_types: str = CQCLClean.imputed_gac_service_types
    imputed_non_res_pir_people_directly_employed: str = (
        "imputed_non_res_pir_people_directly_employed"
    )
    imputed_posts_care_home_model: str = "imputed_posts_care_home_model"
    imputed_posts_non_res_with_dormancy_model: str = (
        "imputed_posts_non_res_with_dormancy_model"
    )
    imputed_filled_post_model: str = "imputed_filled_post_model"
    imputed_filled_posts_per_bed_ratio_model: str = (
        "imputed_filled_posts_per_bed_ratio_model"
    )
    imputed_registration_date: str = CQCLClean.imputed_registration_date
    imputed_regulated_activities: str = CQCLClean.imputed_regulated_activities
    imputed_specialisms: str = CQCLClean.imputed_specialisms
    interpolation_model: str = "interpolation_model"
    last_ascwds_submission: str = "last_ascwds_submission"
    last_filled_posts: str = "last_filled_posts"
    last_pir_submission: str = "last_pir_submission"
    last_rolling_average: str = "last_rolling_average"
    location_id: str = CQCLClean.location_id
    locations_at_provider_count: str = "locations_at_provider_count"
    locations_in_ascwds_at_provider_count: str = "locations_in_ascwds_at_provider_count"
    locations_in_ascwds_with_data_at_provider_count: str = (
        "locations_in_ascwds_with_data_at_provider_count"
    )
    lower_percentile: str = "lower_percentile"
    main_job_group_labelled: str = "main_job_group_labels"
    main_job_role_clean_labelled: str = AWKClean.main_job_role_clean_labelled
    max_filled_posts: str = "max_filled_posts"
    max_filled_posts_per_bed_ratio: str = "max_filled_posts_per_bed_ratio"
    max_residual: str = "max_residual"
    min_filled_posts_per_bed_ratio: str = "min_filled_posts_per_bed_ratio"
    min_residual: str = "min_residual"
    model_name: str = "model_name"
    model_run_timestamp: str = "model_run_timestamp"
    model_version: str = "model_version"
    name: str = CQCLClean.name
    next_submission_time: str = "next_submission_time"
    next_value: str = "next_value"
    next_value_unix_time: str = "next_value_unix_time"
    non_res_pir_linear_regression_model: str = "non_res_pir_linear_regression_model"
    non_res_with_dormancy_model: str = "non_res_with_dormancy_model"
    non_res_without_dormancy_model: str = "non_res_without_dormancy_model"
    number_of_beds: str = CQCLClean.number_of_beds
    number_of_beds_at_provider: str = CQCLClean.number_of_beds + "at_provider"
    number_of_beds_banded: str = "number_of_beds_banded"
    number_of_beds_banded_cleaned: str = number_of_beds_banded + "_cleaned"
    organisation_id: str = AWPClean.organisation_id
    percentage_of_residuals_within_absolute_value: str = (
        "percentage_of_residuals_within_absolute_value"
    )
    percentage_of_residuals_within_absolute_or_percentage_values: str = (
        "percentage_of_residuals_within_absolute_or_percentage_values"
    )
    percentage_of_residuals_within_percentage_value: str = (
        "percentage_of_residuals_within_percentage_value"
    )
    percentage_of_standardised_residuals_within_limit: str = (
        "percentage_of_standardised_residuals_within_limit"
    )
    percentage_residual: str = "percentage_residual"
    person_family_name: str = CQCLClean.person_family_name
    person_given_name: str = CQCLClean.person_given_name
    person_roles: str = CQCLClean.person_roles
    person_title: str = CQCLClean.person_title
    pir_people_directly_employed: str = CQCPIRClean.pir_people_directly_employed
    pir_people_directly_employed_dedup: str = (
        CQCPIRClean.pir_people_directly_employed + "_deduplicated"
    )
    pir_people_directly_employed_filled_posts: str = (
        pir_people_directly_employed + "_filled_posts"
    )
    postcode: str = CQCLClean.postal_code
    posts_rolling_average_model: str = "posts_rolling_average_model"
    potential_grouped_provider: str = "potential_grouped_provider"
    prediction: str = "prediction"
    previous_model_value: str = "previous_model_value"
    previous_non_null_value: str = "previous_non_null_value"
    previous_submission_time: str = "previous_submission_time"
    previous_value: str = "previous_value"
    previous_value_unix_time: str = "previous_value_unix_time"
    primary_service_type: str = CQCLClean.primary_service_type
    proportion_of_time_between_submissions: str = (
        "proportion_of_time_between_submissions"
    )
    provider_id: str = CQCLClean.provider_id
    provider_name: str = CQCLClean.provider_name
    r2: str = "r2"
    registered_manager_count: str = "registered_manager_count"
    registered_manager_names: str = CQCLClean.registered_manager_names
    registration_date: str = CQCLClean.registration_date
    registration_status: str = CQCLClean.registration_status
    related_location: str = CQCLClean.related_location
    residual: str = "residual"
    residuals_ascwds_filled_posts_clean_dedup_non_res_pir: str = (
        "residuals_ascwds_filled_posts_clean_dedup_non_res_pir"
    )
    residuals_estimate_filled_posts_non_res_pir: str = (
        "residuals_estimate_filled_posts_non_res_pir"
    )
    service_count: str = "service_count"
    service_count_capped: str = "service_count_capped"
    services_offered: str = CQCLClean.services_offered
    specialism_count: str = "specialism_count"
    specialisms_offered: str = CQCLClean.specialisms_offered
    standardised_residual: str = "standardised_residual"
    time_registered: str = CQCLClean.time_registered
    total_staff_bounded: str = AWPClean.total_staff_bounded
    unix_time: str = "unix_time"
    upper_percentile: str = "upper_percentile"
    value_unix_time: str = "value_unix_time"
    worker_records_bounded: str = AWPClean.worker_records_bounded


@dataclass
class PrimaryServiceRateOfChangeColumns:
    """The names of the temporary columns created during the rolling average process."""

    care_home_status_count: str = "care_home_status_count"
    column_to_average: str = "column_to_average"
    column_to_average_interpolated: str = "column_to_average_interpolated"
    previous_column_to_average_interpolated: str = (
        "previous_column_to_average_interpolated"
    )
    rolling_current_period_sum: str = "rolling_current_period_sum"
    rolling_previous_period_sum: str = "rolling_previous_period_sum"
    single_period_rate_of_change: str = "single_period_rate_of_change"
    submission_count: str = "submission_count"


@dataclass
class NonResWithAndWithoutDormancyCombinedColumns:
    """The names of the temporary columns used in the combining of the models process."""

    adjustment_ratio: str = "adjustment_ratio"
    avg_with_dormancy: str = "avg_with_dormancy"
    avg_without_dormancy: str = "avg_without_dormancy"
    first_overlap_date: str = "first_overlap_date"
    non_res_without_dormancy_model_adjusted: str = (
        "non_res_without_dormancy_model_adjusted"
    )
    non_res_without_dormancy_model_adjusted_and_residual_applied: str = (
        "non_res_without_dormancy_model_adjusted_and_residual_applied"
    )
    residual_at_overlap: str = "residual_at_overlap"
