from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from utils.ind_cqc_pipeline_column_names import CqcCareDirectoryColumns as ColNames

CQC_CARE_DIRECTORY_SCHEMA = StructType(
    fields=[
        StructField(ColNames.location_id, StringType(), True),
        StructField(ColNames.registration_date, StringType(), True),
        StructField(ColNames.care_home, StringType(), True),
        StructField(ColNames.name, StringType(), True),
        StructField(ColNames.type, StringType(), True),
        StructField(ColNames.phone_number, StringType(), True),
        StructField(ColNames.registered_manager_name, StringType(), True),
        StructField(ColNames.website, StringType(), True),
        StructField(ColNames.number_of_beds, IntegerType(), True),
        StructField(ColNames.region, StringType(), True),
        StructField(ColNames.local_authority, StringType(), True),
        StructField(ColNames.address_line_one, StringType(), True),
        StructField(ColNames.address_line_two, StringType(), True),
        StructField(ColNames.town_or_city, StringType(), True),
        StructField(ColNames.county, StringType(), True),
        StructField(ColNames.postcode, StringType(), True),
        StructField(ColNames.Provider.brand_id, StringType(), True),
        StructField(ColNames.Provider.brand_name, StringType(), True),
        StructField(ColNames.Provider.provider_id, StringType(), True),
        StructField(ColNames.Provider.name, StringType(), True),
        StructField(ColNames.Provider.phone_number, StringType(), True),
        StructField(ColNames.Provider.website, StringType(), True),
        StructField(ColNames.Provider.address_line_one, StringType(), True),
        StructField(ColNames.Provider.adderss_line_two, StringType(), True),
        StructField(ColNames.Provider.town_or_city, StringType(), True),
        StructField(ColNames.Provider.county, StringType(), True),
        StructField(ColNames.Provider.postcode, StringType(), True),
        StructField(ColNames.Provider.nominated_individual, StringType(), True),
        StructField(
            ColNames.RegulatedActivity.accommodation_and_care_in_further_education,
            StringType(),
            True,
        ),
        StructField(
            ColNames.RegulatedActivity.accommodation_and_care,
            StringType(),
            True,
        ),
        StructField(
            ColNames.RegulatedActivity.accommodation_for_substance_misuse,
            StringType(),
            True,
        ),
        StructField(
            ColNames.RegulatedActivity.assessment_under_mental_health_act,
            StringType(),
            True,
        ),
        StructField(
            ColNames.RegulatedActivity.diagnostic_procedures, StringType(), True
        ),
        StructField(ColNames.RegulatedActivity.family_planning, StringType(), True),
        StructField(
            ColNames.RegulatedActivity.management_of_blood_products,
            StringType(),
            True,
        ),
        StructField(
            ColNames.RegulatedActivity.maternity_and_midwifery, StringType(), True
        ),
        StructField(ColNames.RegulatedActivity.nursing_care, StringType(), True),
        StructField(ColNames.RegulatedActivity.personal_care, StringType(), True),
        StructField(ColNames.RegulatedActivity.slimming_clinics, StringType(), True),
        StructField(ColNames.RegulatedActivity.surgical_procedures, StringType(), True),
        StructField(
            ColNames.RegulatedActivity.termination_of_pregnancies, StringType(), True
        ),
        StructField(
            ColNames.RegulatedActivity.transport_services,
            StringType(),
            True,
        ),
        StructField(
            ColNames.RegulatedActivity.treatment_of_disease,
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
