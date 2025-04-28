from dataclasses import dataclass


@dataclass
class DirectPaymentColumnNames:
    # Prepare direct payments
    LA_AREA: str = "la_area_aws"
    YEAR: str = "year"
    SERVICE_USER_DPRS_DURING_YEAR: str = "number_su_dpr_salt"
    CARER_DPRS_DURING_YEAR: str = "number_carer_dpr_salt"
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
    DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF: str = (
        "difference_between_adass_and_total_ascof"
    )
    DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF: str = (
        "difference_between_adass_and_su_only_ascof"
    )
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "proportion_su_only_employing_staff"
    )
    YEAR_AS_INTEGER: str = "year_as_integer"

    # Remove outliers
    OUTLIERS_FOR_REMOVAL: str = "outliers_for_removal"
    COUNT_OF_YEARS_WITH_PROPORTION: str = "count_of_years_with_proportion_by_la_area"
    PENULTIMATE_YEAR_DATA: str = "2021_data"
    LAST_YEAR_CONTAINING_RAW_DATA: str = "last_year_containing_raw_data"
    LAST_RAW_DATA_POINT: str = "last_raw_data_point"
    GROUPED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "avg(proportion_su_only_employing_staff)"
    )
    MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "mean_proportion_su_only_employing_staff_within_la_area"
    )

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

    # Model extrapolation
    EXTRAPOLATION_RATIO: str = "extrapolation_ratio"
    FIRST_YEAR_WITH_DATA: str = "first_year_with_data"
    FIRST_DATA_POINT: str = "first_data_point"
    FIRST_YEAR_MEAN_ESTIMATE: str = "first_year_mean_estimate"
    LAST_YEAR_WITH_DATA: str = "last_year_with_data"
    LAST_DATA_POINT: str = "last_data_point"
    LAST_YEAR_MEAN_ESTIMATE: str = "last_year_mean_estimate"

    # Model interpolation
    FIRST_SUBMISSION_YEAR: str = "first_submission_year"
    LAST_SUBMISSION_YEAR: str = "last_submission_year"
    PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "previous_service_users_employing_staff"
    )
    NEXT_SERVICE_USERS_EMPLOYING_STAFF: str = "next_service_users_employing_staff"
    ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: str = (
        "estimated_proportion_of_service_users_employing_staff_year_provided"
    )
    PREVIOUS_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: (
        str
    ) = "previous_estimated_proportion_of_service_users_employing_staff_year_provided"
    NEXT_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_YEAR_PROVIDED: str = (
        "next_estimated_proportion_of_service_users_employing_staff_year_provided"
    )
    INTERPOLATION_YEAR: str = "interpolation_year"

    # Model using mean
    COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR: str = (
        "count_of_service_user_dprs_during_year"
    )
    SUM_OF_SERVICE_USER_DPRS_DURING_YEAR: str = "sum_of_service_user_dprs_during_year"

    # Rolling average
    COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "count_of_estimated_proportion_of_service_users_employing_staff"
    )
    SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "sum_of_estimated_proportion_of_service_users_employing_staff"
    )
    ROLLING_TOTAL_OF_COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: (
        str
    ) = "rolling_total_of_count_of_estimated_proportion_of_service_users_employing_staff"
    ROLLING_TOTAL_OF_SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: (
        str
    ) = "rolling_total_of_sum_of_estimated_proportion_of_service_users_employing_staff"
    ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "rolling_average_estimated_proportion_of_service_users_employing_staff"
    )

    # Calculate remaining variables
    ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: str = (
        "estimated_service_users_with_self_employed_staff"
    )
    ESTIMATED_CARERS_EMPLOYING_STAFF: str = "estimated_carers_employing_staff"
    ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF: str = "estimated_total_dpr_employing_staff"
    ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: str = (
        "estimated_total_personal_assistant_filled_posts"
    )
    ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF: str = (
        "estimated_proportion_of_total_dpr_employing_staff"
    )
    ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS: str = (
        "estimated_proportion_of_dpr_who_are_service_users"
    )

    # Create summary table
    TOTAL_DPRS: str = "total_dprs"
    PROPORTION_OF_SERVICE_USER_DPRS: str = "proportion_of_service_user_dprs"
    SERVICE_USER_DPRS: str = "service_user_dprs"
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_FINAL: str = (
        "proportion_of_service_users_employing_staff"
    )
    SERVICE_USERS_EMPLOYING_STAFF: str = "service_users_employing_staff"
    SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF: str = (
        "service_users_with_self_employed_staff"
    )
    CARERS_EMPLOYING_STAFF: str = "carers_employing_staff"
    TOTAL_DPRS_EMPLOYING_STAFF: str = "total_dprs_employing_staff"
    TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS: str = "total_personal_assistant_filled_posts"
    PROPORTION_OF_TOTAL_DPRS_EMPLOYING_STAFF: str = (
        "proportion_of_total_dprs_employing_staff"
    )

    # PA ratio
    TOTAL_STAFF_RECODED: str = "total_staff_recoded"
    AVERAGE_STAFF: str = "average_staff"

    RATIO_ROLLING_AVERAGE: str = "ratio_rolling_average"
    COUNT: str = "count"
    COUNT_OF_YEARS: str = "count_of_years"
    SUM_OF_RATIOS: str = "sum_of_ratios"

    HISTORIC_RATIO: str = "historic_ratio"

    # Split PA filled posts by ICB area
    COUNT_OF_DISTINCT_POSTCODES_PER_LA: str = "count_of_distinct_postcodes_per_la"
    HYBRID_AREA_LA_ICB: str = "hybrid_area_la_icb"
    COUNT_OF_DISTINCT_POSTCODES_PER_HYBRID_AREA: str = (
        "count_of_distinct_postcodes_per_hybrid_area"
    )
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
    REMOVE: str = "remove"
    RETAIN: str = "retain"
    PREVIOUS: str = "previous"
    NEXT: str = "next"
