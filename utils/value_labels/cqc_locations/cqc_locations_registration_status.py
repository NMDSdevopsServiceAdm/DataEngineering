from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


@dataclass
class CQCLocationsValueLabelsRegistrationStatus:
    """The possible values of the registration status column in CQC locations data"""

    column_name: str = CQCL.registration_status
    registered: str = "Registered"
    deregistered: str = "Deregistered"

    values_list = [registered, deregistered]
