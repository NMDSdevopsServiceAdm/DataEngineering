from dataclasses import dataclass


@dataclass
class DirectPaymentColumnNames:
    # Prepare direct payments
    la_area: str = "la_area_aws"
    year: str = "year"
    service_user_dprs_during_year: str = "number_su_dpr_salt"
    service_user_dprs_at_year_end: str = "number_su_dpr_year_end_ascof"
    carer_dprs_at_year_end: str = "number_carer_dpr_year_end_ascof"
    dprs_adass: str = "number_of_dprs_adass"
    dprs_employing_staff_adass: str = "number_of_dprs_who_employ_staff_adass"
    proportion_imported: str = "proportion_su_employing_staff_adass"
    historic_service_users_employing_staff_estimate: str = (
        "prev_service_user_employing_staff_proportion"
    )
    filled_posts_per_employer: str = "filled_posts_per_employer"

    # Adass prep
    proportion_dpr_employing_staff: str = "proportion_dpr_employing_staff"
    total_dprs_at_year_end: str = "total_dprs_at_year_end"
    closer_base: str = "closer_base"
    proportion_if_total_dpr_closer: str = "proportion_if_total_dpr_closer"
    proportion_if_service_user_dpr_closer: str = "proportion_if_service_user_dpr_closer"
    proportion_allocated: str = "proportion_allocated"
    proportion_employing_staff: str = "proportion_employing_staff"
    year_as_integer: str = "year_as_integer"

    # Prepare during year data
    total_dprs_during_year: str = "total_dprs_during_year"

    # Estimate service users employing staff
    estimate_using_extrapolation_ratio: str = "estimate_using_extrapolation_ratio"
    estimated_service_users_employing_staff: str = (
        "estimated_service_users_employing_staff"
    )
    estimate_using_mean: str = "estimate_using_mean"
    estimate_using_interpolation: str = "estimate_using_interpolation"
    imputed_proportion_employing_staff: str = "imputed_proportion_employing_staff"
    imputed_proportion_employing_staff_source: str = (
        "imputed_proportion_employing_staff_source"
    )

    # Model extrapolation
    first_year_with_data: str = "first_year_with_data"
    last_year_with_data: str = "last_year_with_data"

    # Rolling average
    rolling_average_proportion_employing_staff: str = (
        "rolling_average_proportion_employing_staff"
    )

    # Calculate remaining variables
    estimated_service_users_employing_self_employed_staff: str = (
        "estimated_service_users_employing_self_employed_staff"
    )
    estimated_total_dpr_employing_staff: str = "estimated_total_dpr_employing_staff"
    estimated_pa_filled_posts: str = "estimated_pa_filled_posts"
    estimated_proportion_of_total_dpr_employing_staff: str = (
        "estimated_proportion_of_total_dpr_employing_staff"
    )

    # Create summary table
    total_dprs: str = "total_dprs"
    service_user_dprs: str = "service_user_dprs"
    employing_staff: str = "employing_staff"
    employing_self_employed_staff: str = "employing_self_employed_staff"
    total_dprs_employing_staff: str = "total_dprs_employing_staff"
    pa_filled_posts: str = "pa_filled_posts"

    # PA ratio
    total_staff_recoded: str = "total_staff_recoded"
    average_staff: str = "average_staff"

    ratio_rolling_average: str = "ratio_rolling_average"

    historic_ratio: str = "historic_ratio"

    # Split PA filled posts by ICB area
    proportion_of_icb_postcodes_in_la_area: str = (
        "proportion_of_icb_postcodes_in_la_area"
    )
    estimate_period_as_date: str = "estimate_period_as_date"
    estimated_pa_filled_posts_per_hybrid_area: str = (
        "estimated_pa_filled_posts_per_hybrid_area"
    )


@dataclass
class DirectPaymentColumnValues:
    total_dprs: str = "total_dprs"
    su_only_dprs: str = "su_only_dprs"
