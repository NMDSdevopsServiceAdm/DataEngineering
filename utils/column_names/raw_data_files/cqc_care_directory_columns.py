from dataclasses import dataclass


@dataclass
class CqcCareDirectoryColumns:
    address_line_one: str = "postaladdressline1"
    address_line_two: str = "postaladdressline2"
    care_home: str = "carehome"
    county: str = "postaladdresscounty"
    local_authority: str = "localauthority"
    location_id: str = "locationid"
    name: str = "name"
    number_of_beds: str = "numberofbeds"
    phone_number: str = "mainphonenumber"
    postcode: str = "postalcode"
    region: str = "region"
    registered_manager_name: str = "registered_manager_name"
    registration_date: str = "registrationdate"
    town_or_city: str = "postaladdresstowncity"
    type: str = "type"
    website: str = "website"

    @dataclass
    class Provider:
        address_line_two: str = "provider_postaladdressline2"
        address_line_one: str = "provider_postaladdressline1"
        brand_id: str = "provider_brandid"
        brand_name: str = "provider_brandname"
        county: str = "provider_postaladdresscounty"
        name: str = "provider_name"
        nominated_individual: str = "provider_nominated_individual_name"
        phone_number: str = "provider_mainphonenumber"
        postcode: str = "provider_postalcode"
        provider_id: str = "providerid"
        town_or_city: str = "provider_postaladdresstowncity"
        website: str = "provider_website"

    @dataclass
    class RegulatedActivity:
        accommodation_and_care: str = "Regulated_activity_Accommodation_for_persons_who_require_nursing_or_personal_care"
        accommodation_and_care_in_further_education: str = "Regulated_activity_Accommodation_and_nursing_or_personal_care_in_the_further_education_sector"
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
        acute_with_overnight_beds: str = (
            "Service_type_Acute_services_with_overnight_beds"
        )
        acute_without_overnight_beds: str = "Service_type_Acute_services_without_overnight_beds__listed_acute_services_with_or_without_overnight_beds"
        ambulance: str = "Service_type_Ambulance_service"
        blood_and_transplant: str = "Service_type_Blood_and_Transplant_service"
        care_home_with_nursing: str = "Service_type_Care_home_service_with_nursing"
        care_home_without_nursing: str = (
            "Service_type_Care_home_service_without_nursing"
        )
        community_healthcare: str = "Service_type_Community_healthcare_service"
        community_mental_health: str = (
            "Service_type_Community_based_services_for_people_with_mental_health_needs"
        )
        community_learning_disability: str = "Service_type_Community_based_services_for_people_with_a_learning_disability"
        community_substance_misuse: str = (
            "Service_type_Community_based_services_for_people_who_misuse_substances"
        )
        dental: str = "Service_type_Dental_service"
        diagnostics: str = "Service_type_Diagnostic_andor_screening_service"
        diagnostic_sessional: str = "Service_type_Diagnostic_andor_screening_service_single_handed_sessional_providers"
        doctors_consultation: str = "Service_type_Doctors_consultation_service"
        doctors_treatment: str = "Service_type_Doctors_treatment_service"
        domiciliary_care: str = "Service_type_Domiciliary_care_service"
        extra_care_housing: str = "Service_type_Extra_Care_housing_services"
        hospice: str = "Service_type_Hospice_services"
        hospice_at_home: str = "Service_type_Hospice_services_at_home"
        hospital_for_mental_health_learning_disability: str = "Service_type_Hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_problems_with_substance_misuse"
        hyperbaric_chamber: str = "Service_type_Hyperbaric_Chamber"
        long_term_conditions: str = "Service_type_Long_term_conditions_services"
        mobile_doctors: str = "Service_type_Mobile_doctors_service"
        nursing_agency: str = (
            "Service_type_Community_health_care_services_Nurses_Agency_only"
        )
        prison_healthcare: str = "Service_type_Prison_Healthcare_Services"
        rehabilitation: str = "Service_type_Rehabilitation_services"
        remote_clinical_advice: str = "Service_type_Remote_clinical_advice_service"
        residential_substance_misuse: str = "Service_type_Residential_substance_misuse_treatment_andor_rehabilitation_service"
        shared_lives: str = "Service_type_Shared_Lives"
        speciallist_college: str = "Service_type_Specialist_college_service"
        supported_living: str = "Service_type_Supported_living_service"
        urgent_care: str = "Service_type_Urgent_care_services"

    @dataclass
    class ServiceUserBand:
        children: str = "Service_user_band_Children_0-18_years"
        dementia: str = "Service_user_band_Dementia"
        eating_disorder: str = "Service_user_band_People_with_an_eating_disorder"
        learning_disabilities_autism: str = (
            "Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder"
        )
        mental_health: str = "Service_user_band_Mental_Health"

        mental_health_act: str = (
            "Service_user_band_People_detained_under_the_Mental_Health_Act"
        )
        misuse_drugs_alcohol: str = (
            "Service_user_band_People_who_misuse_drugs_and_alcohol"
        )
        older_people: str = "Service_user_band_Older_People"

        physical_disability: str = "Service_user_band_Physical_Disability"
        sensory_impairment: str = "Service_user_band_Sensory_Impairment"
        whole_population: str = "Service_user_band_Whole_Population"
        younger_adults: str = "Service_user_band_Younger_Adults"
