from dataclasses import dataclass


@dataclass
class DirectPaymentColumnNames:

    LA_AREA: str = "la_area_aws"
    YEAR: str = "year"
    PROPORTION_OF_DPR_EMPLOYING_STAFF: str = "proportion_dpr_employing_staff_adass"
    SERVICE_USER_DPRS_DURING_YEAR: str = "number_su_dpr_salt"
    CARER_DPRS_DURING_YEAR: str = "number_carer_dpr_salt"
    SERVICE_USER_DPRS_AT_YEAR_END: str = "number_su_dpr_year_end_ascof"
    CARER_DPRS_AT_YEAR_END: str = "number_carer_dpr_year_end_ascof"
    IMD_SCORE: str = "imd_2010"
    DPRS_ADASS: str = "number_of_dprs_adass"
    DPRS_EMPLOYING_STAFF_ADASS: str = "number_of_dprs_who_employ_staff_adass"
    TOTAL_DPRS_DURING_YEAR: str = "total_dpr"
    SERVICE_USERS_EMPLOYING_STAFF: str = "service_users_employing_staff"
    CARERS_EMPLOYING_STAFF: str = "carers_emplying_staff"
    SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF: str = "service_users_and_carers_employing_staff"
    DIFFERENCE_IN_BASES: str = "difference_in_bases_between_adass_and_ascof"
    METHOD: str = "method"
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = "proportion_su_only_employing_staff"
