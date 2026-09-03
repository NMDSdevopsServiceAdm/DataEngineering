from dataclasses import dataclass


@dataclass
class PublicationColumns:
    ct_has_data: str = "ct_has_data"
    consistent_service: str = "consistent_service"
    ct_dispersion_filter: str = "ct_dispersion_filter"
