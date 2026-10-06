from dataclasses import dataclass


@dataclass
class DirectPaymentColumnNames:
    # Prepare direct payments
    LA_AREA: str = "la_area_aws"
    YEAR: str = "year"
    SERVICE_USER_DPRS_DURING_YEAR: str = "number_su_dpr_salt"
    SERVICE_USER_DPRS_AT_YEAR_END: str = "number_su_dpr_year_end_ascof"
    CARER_DPRS_AT_YEAR_END: str = "number_carer_dpr_year_end_ascof"
    DPRS_ADASS: str = "number_of_dprs_adass"
    DPRS_EMPLOYING_STAFF_ADASS: str = "number_of_dprs_who_employ_staff_adass"
    PROPORTION_IMPORTED: str = "proportion_su_employing_staff_adass"
    HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE: str = (
        "prev_service_user_employing_staff_proportion"
    )
    FILLED_POSTS_PER_EMPLOYER: str = "filled_posts_per_employer"

    # Adass prep
    PROPORTION_OF_DPR_EMPLOYING_STAFF: str = "proportion_dpr_employing_staff"
    TOTAL_DPRS_AT_YEAR_END: str = "total_dpr_at_year_end"
    CLOSER_BASE: str = "closer_base"
    PROPORTION_IF_TOTAL_DPR_CLOSER: str = "proportion_if_total_dpr_closer"
    PROPORTION_IF_SERVICE_USER_DPR_CLOSER: str = "proportion_if_service_user_dpr_closer"
    PROPORTION_ALLOCATED: str = "proportion_allocated"
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "proportion_su_only_employing_staff"
    )
    YEAR_AS_INTEGER: str = "year_as_integer"

    # Prepare during year data
    TOTAL_DPRS_DURING_YEAR: str = "total_dpr_during_year"

    # Estimate service users employing staff
    ESTIMATE_USING_EXTRAPOLATION_RATIO: str = "estimate_using_extrapolation_ratio"
    ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: str = (
        "estimated_service_user_dprs_during_year_employing_staff"
    )
    ESTIMATE_USING_MEAN: str = "estimate_using_mean"
    ESTIMATE_USING_INTERPOLATION: str = "estimate_using_interpolation"
    ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "estimated_proportion_of_service_users_employing_staff"
    )
    ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_SOURCE: str = (
        "estimated_proportion_of_service_users_employing_staff_source"
    )

    # Model extrapolation
    FIRST_YEAR_WITH_DATA: str = "first_year_with_data"
    LAST_YEAR_WITH_DATA: str = "last_year_with_data"

    # Rolling average
    ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "rolling_average_estimated_proportion_of_service_users_employing_staff"
    )

    # Calculate remaining variables
    ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: str = (
        "estimated_service_users_with_self_employed_staff"
    )
    ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF: str = "estimated_total_dpr_employing_staff"
    ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: str = (
        "estimated_total_personal_assistant_filled_posts"
    )
    ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF: str = (
        "estimated_proportion_of_total_dpr_employing_staff"
    )

    # Create summary table
    TOTAL_DPRS: str = "total_dprs"
    SERVICE_USER_DPRS: str = "service_user_dprs"
    SERVICE_USERS_EMPLOYING_STAFF: str = "service_users_employing_staff"
    SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: str = (
        "service_users_with_self_employed_staff"
    )
    TOTAL_DPRS_EMPLOYING_STAFF: str = "total_dprs_employing_staff"
    TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: str = "total_personal_assistant_filled_posts"

    # PA ratio
    TOTAL_STAFF_RECODED: str = "total_staff_recoded"
    AVERAGE_STAFF: str = "average_staff"

    RATIO_ROLLING_AVERAGE: str = "ratio_rolling_average"

    HISTORIC_RATIO: str = "historic_ratio"

    # Split PA filled posts by ICB area
    PROPORTION_OF_ICB_POSTCODES_IN_LA_AREA: str = (
        "proportion_of_ICB_postcodes_in_la_area"
    )
    ESTIMATE_PERIOD_AS_DATE: str = "estimate_period_as_date"
    ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS_PER_HYBRID_AREA: str = (
        "estimated_total_personal_assistant_filled_posts_per_hybrid_area"
    )


@dataclass
class DirectPaymentColumnValues:
    TOTAL_DPRS: str = "total_dprs"
    SU_ONLY_DPRS: str = "su_only_dprs"
