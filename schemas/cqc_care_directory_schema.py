from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from utils.ind_cqc_column_names.cqc_care_directory_columns import (
    CqcCareDirectoryColumns as ColNames,
)

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
        StructField(ColNames.ServiceType.acute_with_overnight_beds, StringType(), True),
        StructField(
            ColNames.ServiceType.acute_without_overnight_beds,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceType.ambulance, StringType(), True),
        StructField(ColNames.ServiceType.blood_and_transplant, StringType(), True),
        StructField(ColNames.ServiceType.care_home_with_nursing, StringType(), True),
        StructField(ColNames.ServiceType.care_home_without_nursing, StringType(), True),
        StructField(
            ColNames.ServiceType.community_substance_misuse,
            StringType(),
            True,
        ),
        StructField(
            ColNames.ServiceType.community_learning_disability,
            StringType(),
            True,
        ),
        StructField(
            ColNames.ServiceType.community_mental_health,
            StringType(),
            True,
        ),
        StructField(
            ColNames.ServiceType.nursing_agency,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceType.community_healthcare, StringType(), True),
        StructField(ColNames.ServiceType.dental, StringType(), True),
        StructField(ColNames.ServiceType.diagnostics, StringType(), True),
        StructField(
            ColNames.ServiceType.diagnostic_sessional,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceType.doctors_consultation, StringType(), True),
        StructField(ColNames.ServiceType.doctors_treatment, StringType(), True),
        StructField(ColNames.ServiceType.domiciliary_care, StringType(), True),
        StructField(ColNames.ServiceType.extra_care_housing, StringType(), True),
        StructField(ColNames.ServiceType.hospice, StringType(), True),
        StructField(ColNames.ServiceType.hospice_at_home, StringType(), True),
        StructField(
            ColNames.ServiceType.hospital_for_mental_health_learning_disability,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceType.hyperbaric_chamber, StringType(), True),
        StructField(ColNames.ServiceType.long_term_conditions, StringType(), True),
        StructField(ColNames.ServiceType.mobile_doctors, StringType(), True),
        StructField(ColNames.ServiceType.prison_healthcare, StringType(), True),
        StructField(ColNames.ServiceType.rehabilitation, StringType(), True),
        StructField(ColNames.ServiceType.remote_clinical_advice, StringType(), True),
        StructField(
            ColNames.ServiceType.residential_substance_misuse,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceType.shared_lives, StringType(), True),
        StructField(ColNames.ServiceType.speciallist_college, StringType(), True),
        StructField(ColNames.ServiceType.supported_living, StringType(), True),
        StructField(ColNames.ServiceType.urgent_care, StringType(), True),
        StructField(ColNames.ServiceUserBand.children, StringType(), True),
        StructField(ColNames.ServiceUserBand.dementia, StringType(), True),
        StructField(
            ColNames.ServiceUserBand.learning_disabilities_autism,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceUserBand.mental_health, StringType(), True),
        StructField(ColNames.ServiceUserBand.older_people, StringType(), True),
        StructField(
            ColNames.ServiceUserBand.mental_health_act,
            StringType(),
            True,
        ),
        StructField(ColNames.ServiceUserBand.misuse_drugs_alcohol, StringType(), True),
        StructField(ColNames.ServiceUserBand.eating_disorder, StringType(), True),
        StructField(ColNames.ServiceUserBand.physical_disability, StringType(), True),
        StructField(ColNames.ServiceUserBand.sensory_impairment, StringType(), True),
        StructField(ColNames.ServiceUserBand.whole_population, StringType(), True),
        StructField(ColNames.ServiceUserBand.younger_adults, StringType(), True),
    ]
)
