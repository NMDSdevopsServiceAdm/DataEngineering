from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, collect_set, array, col, regexp_replace, expr
from utils import utils
import sys
import pyspark
import argparse

SPECIALISMS_DICT = {
    "Caring for children": "Service_user_band_Children_0-18_years",
    "Dementia": "Service_user_band_Dementia",
    "Learning disabilities": "Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder",
    "Mental health conditions": "Service_user_band_Mental_Health",
    "Caring for adults over 65 yrs": "Service_user_band_Older_People",
    "Caring for people whose rights are restricted under the Mental Health Act": "Service_user_band_People_detained_under_the_Mental_Health_Act",
    "Substance misuse problems": "Service_user_band_People_who_misuse_drugs_and_alcohol",
    "Eating disorders": "Service_user_band_People_with_an_eating_disorder",
    "Physical disabilities": "Service_user_band_Physical_Disability",
    "Sensory impairment": "Service_user_band_Sensory_Impairment",
    "Services for everyone": "Service_user_band_Whole_Population",
    "Caring for adults under 65 yrs": "Service_user_band_Younger_Adults",
}


def main(source, provider_destination=None, location_destination=None):
    return_datasets = []

    print("Reading CSV from {source}")
    df = utils.read_csv(source)

    print("Formatting date fields")
    df = utils.format_date_fields(df)

    df = df.filter("type=='Social Care Org'")

    print("Create CQC provider parquet file")
    provider_df = unique_providers_with_locations(df)
    distinct_provider_info_df = get_distinct_provider_info(df)
    provider_df = provider_df.join(distinct_provider_info_df, "providerid")

    print(f"Exporting Provider information as parquet to {provider_destination}")
    if provider_destination:
        utils.write_to_parquet(provider_df, provider_destination)
    else:
        return_datasets.append(provider_df)

    print("Create CQC location parquet file")
    location_df = get_general_location_info(df)

    regulatedactivities_df = get_regulatedactivities(df)
    gacservicetypes_df = get_gacservicetypes(df)
    specialisms_df = get_specialisms(df)

    location_df = location_df.join(regulatedactivities_df, "locationid")
    location_df = location_df.join(gacservicetypes_df, "locationid")
    location_df = location_df.join(specialisms_df, "locationid")

    print(f"Exporting Location information as parquet to {location_destination}")
    if location_destination:
        utils.write_to_parquet(location_df, location_destination)
    else:
        return_datasets.append(location_df)

    return return_datasets


def unique_providers_with_locations(df):
    locations_at_prov_df = df.select("providerid", "locationid")
    locations_at_prov_df = (
        locations_at_prov_df.groupby("providerid")
        .agg(collect_set("locationid"))
        .withColumnRenamed("collect_set(locationid)", "locationids")
    )

    return locations_at_prov_df


def get_distinct_provider_info(df):
    prov_info_df = df.selectExpr(
        "providerid",
        "provider_brandid as brandid",
        "provider_brandname as brandname",
        "provider_name as name",
        "provider_mainphonenumber as mainPhoneNumber",
        "provider_website as website",
        "provider_postaladdressline1 as postalAddressLine1",
        "provider_postaladdressline2 as postaladdressline2",
        "provider_postaladdresstowncity as postalAddressTownCity",
        "provider_postaladdresscounty as postalAddressCounty",
        "provider_postalcode as postalCode",
        "provider_nominated_individual_name as nominated_individual_name",
    ).distinct()

    prov_info_df = prov_info_df.withColumn("organisationType", lit("Provider"))
    prov_info_df = prov_info_df.withColumn("registrationstatus", lit("Registered"))

    return prov_info_df


def get_general_location_info(df):
    loc_info_df = df.selectExpr(
        "locationid",
        "providerid",
        "type",
        "name",
        "registrationdate",
        "numberofbeds",
        "website",
        "postaladdressline1",
        "postaladdresstowncity",
        "postaladdresscounty",
        "region",
        "postalcode",
        "carehome",
        "mainphonenumber",
        "localauthority",
    ).distinct()

    loc_info_df = loc_info_df.withColumn("organisationType", lit("Location"))
    loc_info_df = loc_info_df.withColumn("registrationstatus", lit("Registered"))

    return loc_info_df


def get_regulatedactivities(df):
    regulatedactivities_df = df.select(
        "locationid",
        "registered_manager_name",
        "Regulated_activity_Accommodation_and_nursing_or_personal_care_in_the_further_education_sector",
        "Regulated_activity_Accommodation_for_persons_who_require_nursing_or_personal_care",
        "Regulated_activity_Accommodation_for_persons_who_require_treatment_for_substance_misuse",
        "Regulated_activity_Assessment_or_medical_treatment_for_persons_detained_under_the_Mental_Health_Act_1983",
        "Regulated_activity_Diagnostic_and_screening_procedures",
        "Regulated_activity_Family_planning",
        "Regulated_activity_Management_of_supply_of_blood_and_blood_derived_products",
        "Regulated_activity_Maternity_and_midwifery_services",
        "Regulated_activity_Nursing_care",
        "Regulated_activity_Personal_care",
        "Regulated_activity_Services_in_slimming_clinics",
        "Regulated_activity_Surgical_procedures",
        "Regulated_activity_Termination_of_pregnancies",
        "Regulated_activity_Transport_services_triage_and_medical_advice_provided_remotely",
        "Regulated_activity_Treatment_of_disease_disorder_or_injury",
    )

    return regulatedactivities_df


def get_gacservicetypes(df):
    gacservicetypes_df = df.select(
        "locationid",
        "Service_type_Acute_services_with_overnight_beds",
        "Service_type_Acute_services_without_overnight_beds__listed_acute_services_with_or_without_overnight_beds",
        "Service_type_Ambulance_service",
        "Service_type_Blood_and_Transplant_service",
        "Service_type_Care_home_service_with_nursing",
        "Service_type_Care_home_service_without_nursing",
        "Service_type_Community_based_services_for_people_who_misuse_substances",
        "Service_type_Community_based_services_for_people_with_a_learning_disability",
        "Service_type_Community_based_services_for_people_with_mental_health_needs",
        "Service_type_Community_health_care_services_Nurses_Agency_only",
        "Service_type_Community_healthcare_service",
        "Service_type_Dental_service",
        "Service_type_Diagnostic_andor_screening_service",
        "Service_type_Diagnostic_andor_screening_service_single_handed_sessional_providers",
        "Service_type_Doctors_consultation_service",
        "Service_type_Doctors_treatment_service",
        "Service_type_Domiciliary_care_service",
        "Service_type_Extra_Care_housing_services",
        "Service_type_Hospice_services",
        "Service_type_Hospice_services_at_home",
        "Service_type_Hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_problems_with_substance_misuse",
        "Service_type_Hyperbaric_Chamber",
        "Service_type_Long_term_conditions_services",
        "Service_type_Mobile_doctors_service",
        "Service_type_Prison_Healthcare_Services",
        "Service_type_Rehabilitation_services",
        "Service_type_Remote_clinical_advice_service",
        "Service_type_Residential_substance_misuse_treatment_andor_rehabilitation_service",
        "Service_type_Shared_Lives",
        "Service_type_Specialist_college_service",
        "Service_type_Supported_living_service",
        "Service_type_Urgent_care_services",
    )

    # BELOW SHOWS HOW THEY APPEAR IN THE API

    # [Hospital, Acute services with overnight beds]
    # [Clinic, Acute services without overnight beds / listed acute services with or without overnight beds]
    # [Ambulances, Ambulance service]
    # [Blood and transplant service, Blood and Transplant service]
    # [Nursing homes, Care home service with nursing]
    # [Residential homes, Care home service without nursing]
    # [Community services - Substance abuse, Community based services for people who misuse substances]
    # [Community services - Learning disabilities, Community based services for people with a learning disability]
    # [Community services - Mental Health, Community based services for people with mental health needs]
    # [Community services - Nursing, Community health care services - Nurses Agency only]
    # [Community services - Healthcare, Community healthcare service]
    # [Dentist, Dental service]
    # [Diagnosis/screening, Diagnostic and/or screening service]
    # [Diagnosis/screening, Diagnostic and/or screening service - single handed sessional providers]
    # [Doctors/Gps, Doctors consultation service]
    # [Doctors/Gps, Doctors treatment service]
    # [Homecare agencies, Domiciliary care service]
    # [Supported housing, Extra Care housing services]
    # [Hospice, Hospice services]
    # [Home hospice care, Hospice services at home]
    # [Hospitals - Mental health/capacity, Hospital services for people with mental health needs, learning disabilities and problems with substance misuse]
    # [Hyperbaric chamber services, Hyperbaric Chamber]
    # [Long-term conditions, Long term conditions services]
    # [Mobile doctors, Mobile doctors service]
    # [Prison healthcare, Prison Healthcare Services]
    # [Rehabilitation (illness/injury), Rehabilitation services]
    # [Phone/online advice, Remote clinical advice service]
    # [Rehabilitation (substance abuse), Residential substance misuse treatment and/or rehabilitation service]
    # [Shared lives, Shared Lives]
    # [Specialist college service, Specialist college service]
    # [Supported living, Supported living service]
    # [Urgent care centres, Urgent care services]

    return gacservicetypes_df


def replace_value(df, key, value):
    return df.withColumn(value, regexp_replace(value, "Y", key))


def get_specialisms(df):
    df = df.selectExpr(
        "locationid",
        "`Service_user_band_Children_0-18_years`",
        "Service_user_band_Dementia",
        "Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder",
        "Service_user_band_Mental_Health",
        "Service_user_band_Older_People",
        "Service_user_band_People_detained_under_the_Mental_Health_Act",
        "Service_user_band_People_who_misuse_drugs_and_alcohol",
        "Service_user_band_People_with_an_eating_disorder",
        "Service_user_band_Physical_Disability",
        "Service_user_band_Sensory_Impairment",
        "Service_user_band_Whole_Population",
        "Service_user_band_Younger_Adults",
    )

    for new_name, column_name in SPECIALISMS_DICT.items():
        df = replace_value(df, new_name, column_name)

    df = df.select(
        col("locationid"),
        array(
            col("Service_user_band_Children_0-18_years"),
            col("Service_user_band_Dementia"),
            col("Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder"),
            col("Service_user_band_Mental_Health"),
            col("Service_user_band_Older_People"),
            col("Service_user_band_People_detained_under_the_Mental_Health_Act"),
            col("Service_user_band_People_who_misuse_drugs_and_alcohol"),
            col("Service_user_band_People_with_an_eating_disorder"),
            col("Service_user_band_Physical_Disability"),
            col("Service_user_band_Sensory_Impairment"),
            col("Service_user_band_Whole_Population"),
            col("Service_user_band_Younger_Adults"),
        ).alias("specialisms"),
    )

    df = df.withColumn("specialisms", expr("filter(specialisms, elem -> elem != '')"))

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="A CSV file used as source input", required=True)
    parser.add_argument(
        "--provider_destination",
        help="A destination directory for outputting CQC Provider parquet file",
        required=False,
    )
    parser.add_argument(
        "--location_destination",
        help="A destination directory for outputting CQC Location parquet file",
        required=False,
    )

    args, unknown = parser.parse_known_args()

    if args.delimiter:
        print(f"Utilising custom delimiter '{args.delimiter}'")

    return args.source, args.provider_destination, args.location_destination


if __name__ == "__main__":
    print("Spark job 'ingest_cqc_care_directory' starting...")
    print(f"Job parameters: {sys.argv}")

    source, provider_destination, location_destination = collect_arguments()
    main(source, provider_destination, location_destination)

    print("Spark job 'ingest_cqc_care_directory' complete")
