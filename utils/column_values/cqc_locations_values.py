from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)

from utils.column_values.cqc_providers_values import Sector
from utils.column_values.column_values import ColumnValues


@dataclass
class Dormancy(ColumnValues):
    """The possible values of the dormancy column in CQC locations data"""

    dormant: str = "Y"
    not_dormant: str = "N"


dormancy_column_values = Dormancy(CQCL.dormancy)


@dataclass
class LocationType(ColumnValues):
    """The possible values of the type column in CQC locations data"""

    column_name: str = CQCL.type

    social_care_identifier: str = "Social Care Org"
    nhs_healthcare_identifier: str = "NHS Healthcare Organisation"
    independent_healthcare_identifier: str = "Independent Healthcare Org"
    primary_medical_identifier: str = "Primary Medical Services"
    independent_ambulance_identifier: str = "Independent Ambulance"
    primary_dental_identifier: str = "Primary Dental Care"


@dataclass
class RegistrationStatus(ColumnValues):
    """The possible values of the registration status column in CQC locations data"""

    column_name: str = CQCL.registration_status

    registered: str = "Registered"
    deregistered: str = "Deregistered"


@dataclass
class PrimaryServiceType(ColumnValues):
    """The possible values of the primary service type column in CQC locations data"""

    column_name: str = CQCLClean.primary_service_type

    care_home_with_nursing: str = "Care home with nursing"
    care_home_only: str = "Care home without nursing"
    non_residential: str = "non-residential"


@dataclass
class Services(ColumnValues):
    """The possible values of the GAC service types column in CQC locations data"""

    column_name: str = CQCLClean.gac_service_types

    care_home_service_with_nursing: str = "Care home service with nursing"
    care_home_service_without_nursing: str = "Care home service without nursing"
    community_based_services_for_people_who_misuse_substances: str = (
        "Community based services for people who misuse substances"
    )
    hospice_services: str = "Hospice services"
    domiciliary_care_service: str = "Domiciliary care service"
    remote_clinical_advice_service: str = "Remote clinical advice service"
    acute_services_without_overnight_beds: str = (
        "Acute services without overnight beds / listed acute services with or without overnight beds"
    )
    specialist_college_service: str = "Specialist college service"
    ambulance_service: str = "Ambulance service"
    extra_care_housing_services: str = "Extra Care housing services"
    urgent_care_services: str = "Urgent care services"
    supported_living_service: str = "Supported living service"
    prison_healthcare_services: str = "Prison Healthcare Services"
    community_based_services_for_people_with_mental_health_needs: str = (
        "Community based services for people with mental health needs"
    )
    community_healthcare_service: str = "Community healthcare service"
    community_based_services_for_people_with_a_learning_disability: str = (
        "Community based services for people with a learning disability"
    )
    community_health_care_services_nurses_agency_only: str = (
        "Community health care services - Nurses Agency only"
    )
    dental_service: str = "Dental service"
    mobile_doctors_service: str = "Mobile doctors service"
    long_term_conditions_services: str = "Long term conditions services"
    doctors_consultation_service: str = "Doctors consultation service"
    shared_lives: str = "Shared Lives"
    acute_services_with_overnight_beds: str = "Acute services with overnight beds"
    diagnostic_and_screening_service: str = "Diagnostic and/or screening service"
    residential_substance_misuse_treatment_and_rehabilitation_service: str = (
        "Residential substance misuse treatment and/or rehabilitation service"
    )
    rehabilitation_services: str = "Rehabilitation services"
    doctors_treatment_service: str = "Doctors treatment service"
    hospice_services_at_home: str = "Hospice services at home"
    hospital_services_for_people_with_mental_health_needs: str = (
        "Hospital services for people with mental health needs, learning disabilities and problems with substance misuse"
    )
