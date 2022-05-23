from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, collect_set, array, col, split, expr, when, length, struct, explode
from pyspark.sql.types import StringType, FloatType, StructType, ArrayType
from utils import utils
import sys
from schemas import cqc_location_schema, cqc_provider_schema
import argparse


SPECIALISMS_DICT = {
    "Service_user_band_Children_0-18_years": "Caring for children",
    "Service_user_band_Dementia": "Dementia",
    "Service_user_band_Learning_disabilities_or_autistic_spectrum_disorder": "Learning disabilities",
    "Service_user_band_Mental_Health": "Mental health conditions",
    "Service_user_band_Older_People": "Caring for adults over 65 yrs",
    "Service_user_band_People_detained_under_the_Mental_Health_Act": "Caring for people whose rights are restricted under the Mental Health Act",
    "Service_user_band_People_who_misuse_drugs_and_alcohol": "Substance misuse problems",
    "Service_user_band_People_with_an_eating_disorder": "Eating disorders",
    "Service_user_band_Physical_Disability": "Physical disabilities",
    "Service_user_band_Sensory_Impairment": "Sensory impairment",
    "Service_user_band_Whole_Population": "Services for everyone",
    "Service_user_band_Younger_Adults": "Caring for adults under 65 yrs",
}

GACSERVICETYPES_DICT = {
    "Service_type_Acute_services_with_overnight_beds": "Hospital,Acute services with overnight beds",
    "Service_type_Acute_services_without_overnight_beds__listed_acute_services_with_or_without_overnight_beds": "Clinic,Acute services without overnight beds / listed acute services with or without overnight beds",
    "Service_type_Ambulance_service": "Ambulances,Ambulance service",
    "Service_type_Blood_and_Transplant_service": "Blood and transplant service,Blood and Transplant service",
    "Service_type_Care_home_service_with_nursing": "Nursing homes,Care home service with nursing",
    "Service_type_Care_home_service_without_nursing": "Residential homes,Care home service without nursing",
    "Service_type_Community_based_services_for_people_who_misuse_substances": "Community services - Substance abuse,Community based services for people who misuse substances",
    "Service_type_Community_based_services_for_people_with_a_learning_disability": "Community services - Learning disabilities,Community based services for people with a learning disability",
    "Service_type_Community_based_services_for_people_with_mental_health_needs": "Community services - Mental Health,Community based services for people with mental health needs",
    "Service_type_Community_health_care_services_Nurses_Agency_only": "Community services - Nursing,Community health care services - Nurses Agency only",
    "Service_type_Community_healthcare_service": "Community services - Healthcare,Community healthcare service",
    "Service_type_Dental_service": "Dentist,Dental service",
    "Service_type_Diagnostic_andor_screening_service": "Diagnosis/screening,Diagnostic and/or screening service",
    "Service_type_Diagnostic_andor_screening_service_single_handed_sessional_providers": "Diagnosis/screening,Diagnostic and/or screening service - single handed sessional providers",
    "Service_type_Doctors_consultation_service": "Doctors/Gps,Doctors consultation service",
    "Service_type_Doctors_treatment_service": "Doctors/Gps,Doctors treatment service",
    "Service_type_Domiciliary_care_service": "Homecare agencies,Domiciliary care service",
    "Service_type_Extra_Care_housing_services": "Supported housing,Extra Care housing services",
    "Service_type_Hospice_services": "Hospice,Hospice services",
    "Service_type_Hospice_services_at_home": "Home hospice care,Hospice services at home",
    "Service_type_Hospital_services_for_people_with_mental_health_needs_learning_disabilities_and_problems_with_substance_misuse": "Hospitals - Mental health/capacity,Hospital services for people with mental health needs learning disabilities and problems with substance misuse",
    "Service_type_Hyperbaric_Chamber": "Hyperbaric chamber services,Hyperbaric Chamber",
    "Service_type_Long_term_conditions_services": "Long-term conditions,Long term conditions services",
    "Service_type_Mobile_doctors_service": "Mobile doctors,Mobile doctors service",
    "Service_type_Prison_Healthcare_Services": "Prison healthcare,Prison Healthcare Services",
    "Service_type_Rehabilitation_services": "Rehabilitation (illness/injury),Rehabilitation services",
    "Service_type_Remote_clinical_advice_service": "Phone/online advice,Remote clinical advice service",
    "Service_type_Residential_substance_misuse_treatment_andor_rehabilitation_service": "Rehabilitation (substance abuse),Residential substance misuse treatment and/or rehabilitation service",
    "Service_type_Shared_Lives": "Shared lives,Shared Lives",
    "Service_type_Specialist_college_service": "Specialist college service,Specialist college service",
    "Service_type_Supported_living_service": "Supported living,Supported living service",
    "Service_type_Urgent_care_services": "Urgent care centres,Urgent care services",
}

REGULATEDACTIVITIES_DICT = {
    "Regulated_activity_Accommodation_and_nursing_or_personal_care_in_the_further_education_sector": "Accommodation and nursing or personal care in the further education sector,RA0",
    "Regulated_activity_Accommodation_for_persons_who_require_nursing_or_personal_care": "Accommodation for persons who require nursing or personal care,RA2",
    "Regulated_activity_Accommodation_for_persons_who_require_treatment_for_substance_misuse": "Accommodation for persons who require treatment for substance misuse,RA3",
    "Regulated_activity_Assessment_or_medical_treatment_for_persons_detained_under_the_Mental_Health_Act_1983": "Assessment or medical treatment for persons detained under the Mental Health Act 1983,RA6",
    "Regulated_activity_Diagnostic_and_screening_procedures": "Diagnostic and screening procedures,RA8",
    "Regulated_activity_Family_planning": "Family planning,RA15",
    "Regulated_activity_Management_of_supply_of_blood_and_blood_derived_products": "Management of supply of blood and blood derived products,RA9",
    "Regulated_activity_Maternity_and_midwifery_services": "Maternity and midwifery services,RA11",
    "Regulated_activity_Nursing_care": "Nursing care,RA14",
    "Regulated_activity_Personal_care": "Personal care,RA1",
    "Regulated_activity_Services_in_slimming_clinics": "Services in slimming clinics,RA13",
    "Regulated_activity_Surgical_procedures": "Surgical procedures,RA7",
    "Regulated_activity_Termination_of_pregnancies": "Termination of pregnancies,RA12",
    "Regulated_activity_Transport_services_triage_and_medical_advice_provided_remotely": "Transport services triage and medical advice provided remotely, RA10",
    "Regulated_activity_Treatment_of_disease_disorder_or_injury": "Treatment of disease disorder or injury, RA5",
}


def main(source, provider_destination=None, location_destination=None):
    spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    return_datasets = []

    print("Reading CSV from {source}")
    df = utils.read_csv(source)

    print("Formatting date fields")
    df = utils.format_date_fields(df)

    df = df.filter("type=='Social Care Org'")
    df = df.withColumnRenamed("providerid", "providerId").withColumnRenamed("locationid", "locationId")

    print("Create CQC provider parquet file")
    provider_df = unique_providers_with_locations(df)
    distinct_provider_info_df = get_distinct_provider_info(df)
    provider_df = provider_df.join(distinct_provider_info_df, "providerId")

    output_provider_df = spark.createDataFrame(data=[], schema=cqc_provider_schema.PROVIDER_SCHEMA)
    output_provider_df = output_provider_df.unionByName(provider_df, allowMissingColumns=True)

    print(f"Exporting Provider information as parquet to {provider_destination}")
    if provider_destination:
        utils.write_to_parquet(output_provider_df, provider_destination)
    else:
        return_datasets.append(output_provider_df)

    print("Create CQC location parquet file")
    location_df = get_general_location_info(df)

    reg_man_df = reg_man_to_struct(df)

    regulatedactivities_df = reformat_cols(df, REGULATEDACTIVITIES_DICT, "regulatedactivities")
    regulatedactivities_df = regulatedactivities_df.join(reg_man_df, "locationId")
    regulatedactivities_df = regulatedactivities_to_struct(regulatedactivities_df)

    gacservicetypes_df = reformat_cols(df, GACSERVICETYPES_DICT, "gacservicetypes")
    gacservicetypes_df = gacservicetypes_to_struct(gacservicetypes_df)

    specialisms_df = reformat_cols(df, SPECIALISMS_DICT, "specialisms")
    specialisms_df = specialisms_to_struct(specialisms_df)

    location_df = location_df.join(regulatedactivities_df, "locationId")
    location_df = location_df.join(gacservicetypes_df, "locationId")
    location_df = location_df.join(specialisms_df, "locationId")

    output_location_df = spark.createDataFrame(data=[], schema=cqc_location_schema.LOCATION_SCHEMA)
    output_location_df = output_location_df.unionByName(location_df, allowMissingColumns=True)

    print(f"Exporting Location information as parquet to {location_destination}")
    if location_destination:
        utils.write_to_parquet(output_location_df, location_destination)
    else:
        return_datasets.append(output_location_df)

    return return_datasets


def unique_providers_with_locations(df):
    locations_at_prov_df = df.select("providerId", "locationId")
    locations_at_prov_df = (
        locations_at_prov_df.groupby("providerId")
        .agg(collect_set("locationId"))
        .withColumnRenamed("collect_set(locationId)", "locationIds")
    )

    return locations_at_prov_df


def get_distinct_provider_info(df):
    prov_info_df = df.selectExpr(
        "providerId",
        "provider_name as name",
        "provider_mainphonenumber as mainPhoneNumber",
        "provider_postaladdressline1 as postalAddressLine1",
        "provider_postaladdresstowncity as postalAddressTownCity",
        "provider_postaladdresscounty as postalAddressCounty",
        "provider_postalcode as postalCode",
    ).distinct()

    prov_info_df = prov_info_df.withColumn("organisationType", lit("Provider").cast(StringType()))
    prov_info_df = prov_info_df.withColumn("registrationStatus", lit("Registered").cast(StringType()))

    return prov_info_df


def get_general_location_info(df):
    loc_info_df = df.selectExpr(
        "locationId",
        "providerId",
        "type",
        "name",
        "registrationdate as registrationDate",
        "numberofbeds as numberOfBeds",
        "website",
        "postaladdressline1 as postalAddressLine1",
        "postaladdresstowncity as postalAddressTownCity",
        "postaladdresscounty as postalAddressCounty",
        "region",
        "postalcode as postalCode",
        "carehome as careHome",
        "mainphonenumber as mainPhoneNumber",
        "localauthority as localAuthority",
    ).distinct()

    loc_info_df = loc_info_df.withColumn("organisationType", lit("Location"))
    loc_info_df = loc_info_df.withColumn("registrationStatus", lit("Registered"))

    return loc_info_df


def reformat_cols(df, value_mapping_dict, alias):
    column_names = ["locationId"]
    column_names.extend(list(value_mapping_dict.keys()))

    df = df.select(*column_names)

    for new_name, column_name in value_mapping_dict.items():
        df = df.replace("Y", column_name, new_name)
        df = df.withColumn(new_name, split(col(new_name), ",").alias(new_name))

    df = df.select(col("locationId"), array(df.columns[1:]).alias(alias))

    df = df.withColumn(alias, expr("filter(" + alias + ", elem -> elem is not null)"))

    return df


def specialisms_to_struct(df):
    df = df.withColumn("specialisms", expr("transform(specialisms, x-> named_struct('name',x[0]))"))

    return df


def gacservicetypes_to_struct(df):

    df = df.withColumn(
        "gacservicetypes", expr("transform(gacservicetypes, x-> named_struct('name',x[0], 'description',x[1]))")
    )

    return df


def reg_man_to_struct(df):
    df = df.select("locationId", "registered_manager_name")
    df = df.replace("*", None)

    df = df.withColumn("personTitle", when(length(col("registered_manager_name")) > 1, "M").otherwise(lit(None)))
    df = df.withColumn("personGivenName", split(col("registered_manager_name"), ", ").getItem(1))
    df = df.withColumn("personFamilyName", split(col("registered_manager_name"), ",").getItem(0))
    df = df.withColumn(
        "personRoles", when(length(col("registered_manager_name")) > 1, "Registered Manager").otherwise(lit(None))
    )

    df = df.select(
        "locationId", struct("personTitle", "personGivenName", "personFamilyName", "personRoles").alias("contacts")
    )

    df = df.select("locationId", array("contacts").alias("contacts"))

    return df


def regulatedactivities_to_struct(df):
    df = df.select("locationId", "contacts", explode(col("regulatedactivities")).alias("regulatedactivities"))

    df = df.withColumn("name", col("regulatedactivities").getItem(0))
    df = df.withColumn("code", col("regulatedactivities").getItem(1))

    df = df.select("locationId", struct("name", "code", "contacts").alias("regulatedactivities"))

    df = df.groupBy("locationId").agg(collect_set("regulatedactivities").alias("regulatedactivities"))

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

    return args.source, args.provider_destination, args.location_destination


if __name__ == "__main__":
    print("Spark job 'ingest_cqc_care_directory' starting...")
    print(f"Job parameters: {sys.argv}")

    source, provider_destination, location_destination = collect_arguments()
    main(source, provider_destination, location_destination)

    print("Spark job 'ingest_cqc_care_directory' complete")
