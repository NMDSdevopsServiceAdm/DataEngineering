from dataclasses import dataclass


@dataclass
class PublicationColumns:
    ct_total_employed_imputed: str = "ct_total_employed_imputed"
    ct_has_data_long_term: str = "ct_has_data_long_term"
    ct_has_data_medium_term: str = "ct_has_data_medium_term"
    ct_has_data_short_term: str = "ct_has_data_short_term"
    consistent_service: str = "consistent_service"
    ct_dispersion_filter: str = "ct_dispersion_filter"
