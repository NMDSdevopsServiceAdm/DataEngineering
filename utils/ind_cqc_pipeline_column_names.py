from dataclasses import dataclass


@dataclass
class CqcCareDirectoryColumns:
    location_id: str = "locationid"
    registration_date: str = "registrationdate"
    care_home: str = "carehome"
    name: str = "name"
    type: str = "type"
    phone_number: str = "mainphonenumber"
    registered_manager_name: str = "registered_manager_name"
    website: str = "website"
    number_of_beds: str = "numberofbeds"
    region: str = "region"
    local_authority: str = "localauthority"
    address_line_one: str = "postaladdressline1"
    address_line_two: str = "postaladdressline2"
    town_or_city: str = "postaladdresstowncity"
    county: str = "postaladdresscounty"
    postcode: str = "postalcode"

    @dataclass
    class Provider:
        brand_id: str = "provider_brandid"
        brand_name: str = "provider_brandname"
        provider_id: str = "providerid"
        name: str = "provider_name"
        phone_number: str = "provider_mainphonenumber"
        website: str = "provider_website"
        address_line_one: str = "provider_postaladdressline1"
        adderss_line_two: str = "provider_postaladdressline2"
        town_or_city: str = "provider_postaladdresstowncity"
        county: str = "provider_postaladdresscounty"
        postcode: str = "provider_postalcode"
        nominated_individual: str = "provider_nominated_individual_name"

    @dataclass
    class RegulatedActivity:
        accommodation_and_care_in_further_education: str = "Regulated_activity_Accommodation_and_nursing_or_personal_care_in_the_further_education_sector"
        accommodation_and_care: str = "Regulated_activity_Accommodation_for_persons_who_require_nursing_or_personal_care"
        accommodation_for_substance_misuse: str = "Regulated_activity_Accommodation_for_persons_who_require_treatment_for_substance_misuse"
        assessment_under_mental_health_act: str = "Regulated_activity_Assessment_or_medical_treatment_for_persons_detained_under_the_Mental_Health_Act_1983"
        diagnostic_procedures: str = (
            "Regulated_activity_Diagnostic_and_screening_procedures"
        )
        family_planning: str = "Regulated_activity_Family_planning"
        management_of_blood_products: str = "Regulated_activity_Management_of_supply_of_blood_and_blood_derived_products"
        maternity_and_midwifery: str = (
            "Regulated_activity_Maternity_and_midwifery_services"
        )
        nursing_care: str = "Regulated_activity_Nursing_care"
        personal_care: str = "Regulated_activity_Personal_care"
        slimming_clinics: str = "Regulated_activity_Services_in_slimming_clinics"
        surgical_procedures: str = "Regulated_activity_Surgical_procedures"
        termination_of_pregnancies: str = (
            "Regulated_activity_Termination_of_pregnancies"
        )
        transport_services: str = "Regulated_activity_Transport_services_triage_and_medical_advice_provided_remotely"
        treatment_of_disease: str = (
            "Regulated_activity_Treatment_of_disease_disorder_or_injury"
        )
        @dataclass
        class ServiceType:
            acute_with_overnight_beds:str ="Service_type_Acute_services_with_overnight_beds"
            acute_without_overnight_beds:str = "Service_type_Acute_services_without_overnight_beds__listed_acute_services_with_or_without_overnight_beds"
            "Service_type_Ambulance_service"
            "Service_type_Blood_and_Transplant_service"
            "Service_type_Care_home_service_with_nursing"
            "Service_type_Care_home_service_without_nursing"
            "Service_type_Community_based_services_for_people_who_misuse_substances"
            "Service_type_Community_based_services_for_people_with_a_learning_disability"
            "Service_type_Community_based_services_for_people_with_mental_health_needs"
            "Service_type_Community_health_care_services_Nurses_Agency_only"
            "Service_type_Community_healthcare_service"
            "Service_type_Dental_service"
            "Service_type_Diagnostic_andor_screening_service"
            "Service_type_Diagnostic_andor_screening_service_single_handed_sessional_providers"
            "Service_type_Doctors_consultation_service"
            "Service_type_Doctors_treatment_service"
            "Service_type_Domiciliary_care_service"
            "Service_type_Extra_Care_housing_services"
            "Service_type_Hospice_services"
            "Service_type_Hospice_services_at_home"
            "Service_type_Hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_problems_with_substance_misuse"
            "Service_type_Hyperbaric_Chamber"
            "Service_type_Long_term_conditions_services"
            "Service_type_Mobile_doctors_service"
            "Service_type_Prison_Healthcare_Services"
            "Service_type_Rehabilitation_services"
            "Service_type_Remote_clinical_advice_service"
            "Service_type_Residential_substance_misuse_treatment_andor_rehabilitation_service"
            "Service_type_Shared_Lives"
            "Service_type_Specialist_college_service"
            "Service_type_Supported_living_service"
            "Service_type_Urgent_care_services"
