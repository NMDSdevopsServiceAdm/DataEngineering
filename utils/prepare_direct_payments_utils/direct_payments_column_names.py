from dataclasses import dataclass


@dataclass
class DirectPaymentColumnNames:

    LA_AREA: str = "la_area_aws"
    YEAR: str = "year"
    PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF: str = "proportion_su_employing_staff_adass"
    SERVICE_USER_DPRS_DURING_YEAR: str = "number_su_dpr_salt"
    CARER_DPRS_DURING_YEAR: str = "number_carer_dpr_salt"
    SERVICE_USER_DPRS_AT_YEAR_END: str = "number_su_dpr_year_end_ascof"
    CARER_DPRS_AT_YEAR_END: str = "number_carer_dpr_year_end_ascof"
    IMD_SCORE: str = "imd_2010"
    DPRS_ADASS: str = "number_of_dprs_adass"
    DPRS_EMPLOYING_STAFF_ADASS: str = "number_of_dprs_who_employ_staff_adass"
