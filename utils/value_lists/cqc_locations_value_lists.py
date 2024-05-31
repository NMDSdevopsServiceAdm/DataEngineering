from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)

from utils.value_lists.cqc_providers_value_lists import Sector


@dataclass
class Dormancy:
    """The possible values of the dormancy column in CQC locations data"""

    column_name: str = CQCL.dormancy

    dormant: str = "Y"
    not_dormant: str = "N"

    values_list = [dormant, not_dormant]


@dataclass
class LocationType:
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


@dataclass
class RegistrationStatus:
    """The possible values of the registration status column in CQC locations data"""

    column_name: str = CQCL.registration_status

    registered: str = "Registered"
    deregistered: str = "Deregistered"

    values_list = [registered, deregistered]


@dataclass
class PrimaryServiceType:
    """The possible values of the primary service type column in CQC locations data"""

    column_name: str = CQCLClean.primary_service_type

    care_home_with_nursing: str = "Care home with nursing"
    care_home_only: str = "Care home without nursing"
    non_residential: str = "non-residential"

    values_list = [care_home_with_nursing, care_home_only, non_residential]
