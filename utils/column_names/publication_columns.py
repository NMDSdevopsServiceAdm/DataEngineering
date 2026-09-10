from dataclasses import dataclass


@dataclass
class PublicationColumns:
    ct_care_home_has_data_long_term: str = "ct_care_home_has_data_long_term"
    ct_care_home_has_data_medium_term: str = "ct_care_home_has_data_medium_term"
    ct_care_home_has_data_short_term: str = "ct_care_home_has_data_short_term"
    ct_non_res_has_data_long_term: str = "ct_non_res_has_data_long_term"
    ct_non_res_has_data_medium_term: str = "ct_non_res_has_data_medium_term"
    ct_non_res_has_data_short_term: str = "ct_non_res_has_data_short_term"
    consistent_service: str = "consistent_service"
    ct_dispersion_filter: str = "ct_dispersion_filter"
