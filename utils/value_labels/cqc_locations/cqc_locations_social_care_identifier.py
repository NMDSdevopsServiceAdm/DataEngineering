from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


@dataclass
class CQCLocationsValueLabelsType:
    """The possible values of the type column in CQC locations data"""

    column_name: str = CQCL.type
    social_care_identifier: str = "Social Care Org"
    nhs_healthcare_identifier: str = "NHS Healthcare Organisation"
    independent_healthcare_identifier: str = "Independent Healthcare Org"
    primary_medical_identifier: str = "Primary Medical Services"
    independent_ambulance_identifier: str = "Independent Ambulance"
    primary_dental_identifier: str = "Primary Dental Care"

    values_list = [
        social_care_identifier,
        nhs_healthcare_identifier,
        independent_healthcare_identifier,
        primary_medical_identifier,
        independent_ambulance_identifier,
        primary_dental_identifier,
    ]
