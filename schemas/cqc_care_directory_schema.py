from pyspark.sql.types import StructField, StructType, StringType, IntegerType

CQC_CARE_DIRECTORY_SCHEMA = StructType(
    fields=[
        StructField("locationid", StringType(), True),
        StructField("registrationdate", StringType(), True),
        StructField("carehome", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("mainphonenumber", StringType(), True),
        StructField("registered_manager_name", StringType(), True),
        StructField("website", StringType(), True),
        StructField("numberofbeds", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("localauthority", StringType(), True),
        StructField("postaladdressline1", StringType(), True),
        StructField("postaladdressline2", StringType(), True),
        StructField("postaladdresstowncity", StringType(), True),
        StructField("postaladdresscounty", StringType(), True),
        StructField("postalcode", StringType(), True),
        StructField("provider_brandid", StringType(), True),
        StructField("provider_brandname", StringType(), True),
        StructField("providerid", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("provider_mainphonenumber", StringType(), True),
        StructField("provider_website", StringType(), True),
        StructField("provider_postaladdressline1", StringType(), True),
        StructField("provider_postaladdressline2", StringType(), True),
        StructField("provider_postaladdresstowncity", StringType(), True),
        StructField("provider_postaladdresscounty", StringType(), True),
        StructField("provider_postalcode", StringType(), True),
        StructField("provider_nominated_individual_name", StringType(), True),
        StructField(
            "Regulated_activity_Accommodation_and_nursing_or_personal_care_in_the_further_education_sector",
            StringType(),
            True,
        ),
        StructField(
            "Regulated_activity_Accommodation_for_persons_who_require_nursing_or_personal_care",
            StringType(),
            True,
        ),
        StructField(
            "Regulated_activity_Accommodation_for_persons_who_require_treatment_for_substance_misuse",
            StringType(),
            True,
        ),
        StructField(
            "Regulated_activity_Assessment_or_medical_treatment_for_persons_detained_under_the_Mental_Health_Act_1983",
            StringType(),
            True,
        ),
        StructField(
            "Regulated_activity_Diagnostic_and_screening_procedures", StringType(), True
        ),
        StructField("Regulated_activity_Family_planning", StringType(), True),
        StructField(
            "Regulated_activity_Management_of_supply_of_blood_and_blood_derived_products",
            StringType(),
            True,
        ),
        StructField(
            "Regulated_activity_Maternity_and_midwifery_services", StringType(), True
        ),
        StructField("Regulated_activity_Nursing_care", StringType(), True),
        StructField("Regulated_activity_Personal_care", StringType(), True),
        StructField(
            "Regulated_activity_Services_in_slimming_clinics", StringType(), True
        ),
        StructField("Regulated_activity_Surgical_procedures", StringType(), True),
        StructField(
            "Regulated_activity_Termination_of_pregnancies", StringType(), True
        ),
        StructField(
            "Regulated_activity_Transport_services_triage_and_medical_advice_provided_remotely",
            StringType(),
            True,
        ),
        StructField(
            "Regulated_activity_Treatment_of_disease_disorder_or_injury",
            StringType(),
            True,
        ),
        StructField(
            "Service_type_Acute_services_with_overnight_beds", StringType(), True
        ),
        StructField(
            "Service_type_Acute_services_without_overnight_beds__listed_acute_services_with_or_without_overnight_beds",
            StringType(),
            True,
        ),
        StructField("Service_type_Ambulance_service", StringType(), True),
        StructField("Service_type_Blood_and_Transplant_service", StringType(), True),
        StructField("Service_type_Care_home_service_with_nursing", StringType(), True),
        StructField(
            "Service_type_Care_home_service_without_nursing", StringType(), True
        ),
        StructField(
            "Service_type_Community_based_services_for_people_who_misuse_substances",
            StringType(),
            True,
        ),
        StructField(
            "Service_type_Community_based_services_for_people_with_a_learning_disability",
            StringType(),
            True,
        ),
        StructField(
            "Service_type_Community_based_services_for_people_with_mental_health_needs",
            StringType(),
            True,
        ),
        StructField(
            "Service_type_Community_health_care_services_Nurses_Agency_only",
            StringType(),
            True,
        ),
        StructField("Service_type_Community_healthcare_service", StringType(), True),
        StructField("Service_type_Dental_service", StringType(), True),
        StructField(
            "Service_type_Diagnostic_andor_screening_service", StringType(), True
        ),
        StructField(
            "Service_type_Diagnostic_andor_screening_service_single_handed_sessional_providers",
            StringType(),
            True,
        ),
        StructField("Service_type_Doctors_consultation_service", StringType(), True),
        StructField("Service_type_Doctors_treatment_service", StringType(), True),
        StructField("Service_type_Domiciliary_care_service", StringType(), True),
        StructField("Service_type_Extra_Care_housing_services", StringType(), True),
        StructField("Service_type_Hospice_services", StringType(), True),
        StructField("Service_type_Hospice_services_at_home", StringType(), True),
        StructField(
            "Service_type_Hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_problems_with_substance_misuse",
            StringType(),
            True,
        ),
        StructField("Service_type_Hyperbaric_Chamber", StringType(), True),
        StructField("Service_type_Long_term_conditions_services", StringType(), True),
        StructField("Service_type_Mobile_doctors_service", StringType(), True),
        StructField("Service_type_Prison_Healthcare_Services", StringType(), True),
        StructField("Service_type_Rehabilitation_services", StringType(), True),
        StructField("Service_type_Remote_clinical_advice_service", StringType(), True),
        StructField(
            "Service_type_Residential_substance_misuse_treatment_andor_rehabilitation_service",
            StringType(),
            True,
        ),
        StructField("Service_type_Shared_Lives", StringType(), True),
        StructField("Service_type_Specialist_college_service", StringType(), True),
        StructField("Service_type_Supported_living_service", StringType(), True),
        StructField("Service_type_Urgent_care_services", StringType(), True),
        StructField("Service_user_band_Children_0-18_years", StringType(), True),
        StructField("Service_user_band_Dementia", StringType(), True),
        StructField(
            "Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder",
            StringType(),
            True,
        ),
        StructField("Service_user_band_Mental_Health", StringType(), True),
        StructField("Service_user_band_Older_People", StringType(), True),
        StructField(
            "Service_user_band_People_detained_under_the_Mental_Health_Act",
            StringType(),
            True,
        ),
        StructField(
            "Service_user_band_People_who_misuse_drugs_and_alcohol", StringType(), True
        ),
        StructField(
            "Service_user_band_People_with_an_eating_disorder", StringType(), True
        ),
        StructField("Service_user_band_Physical_Disability", StringType(), True),
        StructField("Service_user_band_Sensory_Impairment", StringType(), True),
        StructField("Service_user_band_Whole_Population", StringType(), True),
        StructField("Service_user_band_Younger_Adults", StringType(), True),
    ]
)
