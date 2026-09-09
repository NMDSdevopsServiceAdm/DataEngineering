from dataclasses import dataclass


@dataclass
class PublicationColumns:
    ct_care_home_has_data_2021: str = "ct_care_home_has_data_2021"
    ct_care_home_has_data_2025: str = "ct_care_home_has_data_2025"
    ct_care_home_has_data_2026: str = "ct_care_home_has_data_2026"
    ct_non_res_has_data_2021: str = "ct_non_res_has_data_2021"
    ct_non_res_has_data_2025: str = "ct_non_res_has_data_2025"
    ct_non_res_has_data_2026: str = "ct_non_res_has_data_2026"
    consistent_service: str = "consistent_service"
    ct_dispersion_filter: str = "ct_dispersion_filter"
