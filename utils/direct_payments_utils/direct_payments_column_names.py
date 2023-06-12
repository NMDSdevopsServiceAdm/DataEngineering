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

    TOTAL_DPRS_DURING_YEAR: str = "total_dpr_during_year"

    ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF: str = (
        "estimated_service_user_dprs_during_year_employing_staff"
    )


@dataclass
class DirectPaymentColumnValues:
    ADASS_INCLUDES_CARERS: str = "adass includes carers"
    ADASS_DOES_NOT_INCLUDE_CARERS: str = "adass does not include carers"
