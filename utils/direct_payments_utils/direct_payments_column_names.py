from dataclasses import dataclass


@dataclass
class DirectPaymentColumnNames:

    LA_AREA: str = "la_area_aws"
    YEAR: str = "year"
    SERVICE_USER_DPRS_DURING_YEAR: str = "number_su_dpr_salt"
    CARER_DPRS_DURING_YEAR: str = "number_carer_dpr_salt"
    SERVICE_USER_DPRS_AT_YEAR_END: str = "number_su_dpr_year_end_ascof"
    CARER_DPRS_AT_YEAR_END: str = "number_carer_dpr_year_end_ascof"
    IMD_SCORE: str = "imd_2010"
    DPRS_ADASS: str = "number_of_dprs_adass"
    DPRS_EMPLOYING_STAFF_ADASS: str = "number_of_dprs_who_employ_staff_adass"
    PROPORTION_IMPORTED: str = "proportion_dpr_employing_staff_adass"

    PROPORTION_OF_DPR_EMPLOYING_STAFF: str = "proportion_dpr_employing_staff"
    TOTAL_DPRS_AT_YEAR_END: str = "total_dpr_at_year_end"
    SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END: str = (
        "service_users_employing_staff_at_year_end"
    )
    CARERS_EMPLOYING_STAFF_AT_YEAR_END: str = "carers_employing_staff_at_year_end"
    SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END: str = (
        "service_users_and_carers_employing_staff_at_year_end"
    )
    DIFFERENCE_IN_BASES: str = "difference_in_bases_between_adass_and_ascof"
    METHOD: str = "method"
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "proportion_su_only_employing_staff"
    )
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP: str = (
        "proportion_su_only_employing_staff_temp"
    )

    GROUPED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "avg(proportion_su_only_employing_staff)"
    )
    MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "mean_proportion_su_only_employing_staff_within_la_area"
    )

    TOTAL_DPRS_DURING_YEAR: str = "total_dpr_during_year"

    ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: str = (
        "estimated_service_user_dprs_during_year_employing_staff"
    )
    ESTIMATE_USING_MEAN: str = "estimate_using_mean"

    YEAR_AS_INTEGER: str = "year_as_integer"
    FIRST_YEAR_WITH_DATA: str = "first_year_with_data"
    FIRST_DATA_POINT: str = "first_data_point"
    COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR: str = (
        "count_of_service_user_dprs_during_year"
    )
    SUM_OF_SERVICE_USER_DPRS_DURING_YEAR: str = "sum_of_service_user_dprs_during_year"
    ROLLING_TOTAL_COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR: str = (
        "rolling_total_count_of_service_user_dprs_during_year"
    )
    ROLLING_TOTAL_SUM_OF_SERVICE_USER_DPRS_DURING_YEAR: str = (
        "rolling_total_sum_of_service_user_dprs_during_year"
    )
    # ROLLING_AVERAGE: str = "rolling_average"
    # FIRST_YEAR_ROLLING_AVERAGE: str = "first_year_rolling_average"
    FIRST_YEAR_MEAN_ESTIMATE: str = "first_year_mean_estimate"
    EXTRAPOLATION_RATIO: str = "extrapolation_ratio"
    ESTIMATE_USING_EXTRAPOLATION_RATIO: str = "estimate_using_extrapolation_ratio"
    LAST_YEAR_WITH_DATA: str = "last_year_with_data"
    LAST_DATA_POINT: str = "last_data_point"
    LAST_YEAR_MEAN_ESTIMATE: str = "last_year_mean_estimate"

    FIRST_SUBMISSION_YEAR: str = "first_submission_year"
    LAST_SUBMISSION_YEAR: str = "last_submission_year"
    PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "previous_service_users_employing_staff"
    )
    NEXT_SERVICE_USERS_EMPLOYING_STAFF: str = "next_service_users_employing_staff"
    SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA: str = (
        "service_users_employing_staff_year_with_data"
    )
    PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA: str = (
        "previous_service_users_employing_staff_year_with_data"
    )
    NEXT_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA: str = (
        "next_service_users_employing_staff_year_with_data"
    )
    ESTIMATE_USING_INTERPOLATION: str = "estimate_using_interpolation"
    INTERPOLATION_YEAR: str = "interpolation_year"

    ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = (
        "estimated_proportion_of_service_users_employing_staff"
    )


@dataclass
class DirectPaymentColumnValues:
    ADASS_INCLUDES_CARERS: str = "adass includes carers"
    ADASS_DOES_NOT_INCLUDE_CARERS: str = "adass does not include carers"
