import sys
import argparse

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType

from utils import utils, cqc_care_directory_dictionaries
from schemas import cqc_location_schema, cqc_provider_schema


def main(source, provider_destination, location_destination):
    df = get_cqc_care_directory(source)

    provider_df = convert_to_cqc_provider_api_format(df)
    export_parquet_files(provider_df, provider_destination)

    location_df = convert_to_cqc_location_api_format(df)
    export_parquet_files(location_df, location_destination)


def get_cqc_care_directory(source):
    print(f"Reading CSV from {source}")
    df = utils.read_csv(source)

    print("Formatting date fields")
    df = utils.format_date_fields(df, raw_date_format="dd/MM/yyyy")

    df = df.filter("type=='Social Care Org'")
    df = df.withColumnRenamed("providerid", "providerId").withColumnRenamed(
        "locationid", "locationId"
    )

    return df


def convert_to_cqc_provider_api_format(df):
    spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    print("Create CQC provider parquet file")
    provider_df = unique_providerids_with_array_of_their_locationids(df)
    distinct_provider_info_df = get_distinct_provider_info(df)
    provider_df = provider_df.join(distinct_provider_info_df, "providerId")

    output_provider_df = spark.createDataFrame(
        data=[], schema=cqc_provider_schema.PROVIDER_SCHEMA
    )
    output_provider_df = output_provider_df.unionByName(
        provider_df, allowMissingColumns=True
    )

    return output_provider_df


def unique_providerids_with_array_of_their_locationids(df):
    locations_at_prov_df = df.select("providerId", "locationId")
    locations_at_prov_df = (
        locations_at_prov_df.groupby("providerId")
        .agg(F.collect_set("locationId"))
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

    prov_info_df = prov_info_df.withColumn(
        "organisationType", F.lit("Provider").cast(StringType())
    )
    prov_info_df = prov_info_df.withColumn(
        "registrationStatus", F.lit("Registered").cast(StringType())
    )

    return prov_info_df


def convert_to_cqc_location_api_format(df):
    spark = SparkSession.builder.appName("test_ingest_cqc_care_directory").getOrCreate()

    print("Create CQC location parquet file")
    location_df = get_general_location_info(df)

    reg_man_df = create_contacts_from_registered_manager_name(df)

    regulated_activities_df = convert_multiple_boolean_columns_into_single_array(
        df,
        cqc_care_directory_dictionaries.REGULATED_ACTIVITIES_DICT,
        "regulatedactivities",
    )
    regulated_activities_df = regulated_activities_df.join(reg_man_df, "locationId")
    regulated_activities_df = convert_regulated_activities_to_struct(
        regulated_activities_df
    )

    gac_service_types_df = convert_multiple_boolean_columns_into_single_array(
        df, cqc_care_directory_dictionaries.GAC_SERVICE_TYPES_DICT, "gacservicetypes"
    )
    gac_service_types_df = convert_gac_service_types_to_struct(gac_service_types_df)

    specialisms_df = convert_multiple_boolean_columns_into_single_array(
        df, cqc_care_directory_dictionaries.SPECIALISMS_DICT, "specialisms"
    )
    specialisms_df = convert_specialisms_to_struct(specialisms_df)

    location_df = location_df.join(regulated_activities_df, "locationId")
    location_df = location_df.join(gac_service_types_df, "locationId")
    location_df = location_df.join(specialisms_df, "locationId")

    output_location_df = spark.createDataFrame(
        data=[], schema=cqc_location_schema.LOCATION_SCHEMA
    )
    output_location_df = output_location_df.unionByName(
        location_df, allowMissingColumns=True
    )

    return output_location_df


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

    loc_info_df = loc_info_df.withColumn(
        "numberOfBeds", F.col("numberOfBeds").cast(IntegerType())
    )
    loc_info_df = loc_info_df.withColumn("organisationType", F.lit("Location"))
    loc_info_df = loc_info_df.withColumn("registrationStatus", F.lit("Registered"))

    return loc_info_df


def convert_multiple_boolean_columns_into_single_array(df, value_mapping_dict, alias):
    column_names = ["locationId"]
    column_names.extend(list(value_mapping_dict.keys()))

    df = df.select(*column_names)

    for new_name, column_name in value_mapping_dict.items():
        df = df.replace("Y", column_name, new_name)
        df = df.withColumn(new_name, F.split(F.col(new_name), ",").alias(new_name))

    df = df.select(F.col("locationId"), F.array(df.columns[1:]).alias(alias))

    df = df.withColumn(alias, F.expr("filter(" + alias + ", elem -> elem is not null)"))

    return df


def convert_specialisms_to_struct(df):
    df = df.withColumn(
        "specialisms", F.expr("transform(specialisms, x-> named_struct('name',x[0]))")
    )

    return df


def convert_gac_service_types_to_struct(df):
    df = df.withColumn(
        "gacservicetypes",
        F.expr(
            "transform(gacservicetypes, x-> named_struct('name',x[0], 'description',x[1]))"
        ),
    )

    return df


def create_contacts_from_registered_manager_name(df):
    df = df.select("locationId", "registered_manager_name")
    df = df.replace("*", None)

    df = df.withColumn(
        "personTitle",
        F.when(F.length(F.col("registered_manager_name")) > 1, "M").otherwise(
            F.lit(None)
        ),
    )
    df = df.withColumn(
        "personGivenName", F.split(F.col("registered_manager_name"), ", ").getItem(1)
    )
    df = df.withColumn(
        "personFamilyName", F.split(F.col("registered_manager_name"), ",").getItem(0)
    )
    df = df.withColumn(
        "personRoles",
        F.when(
            F.length(F.col("registered_manager_name")) > 1, "Registered Manager"
        ).otherwise(F.lit(None)),
    )

    df = df.select(
        "locationId",
        F.struct(
            "personTitle", "personGivenName", "personFamilyName", "personRoles"
        ).alias("contacts"),
    )

    df = df.select("locationId", F.array("contacts").alias("contacts"))

    return df


def convert_regulated_activities_to_struct(df):
    df = df.select(
        "locationId",
        "contacts",
        F.explode(F.col("regulatedactivities")).alias("regulatedactivities"),
    )

    df = df.withColumn("name", F.col("regulatedactivities").getItem(0))
    df = df.withColumn("code", F.col("regulatedactivities").getItem(1))

    df = df.select(
        "locationId", F.struct("name", "code", "contacts").alias("regulatedactivities")
    )

    df = df.groupBy("locationId").agg(
        F.collect_set("regulatedactivities").alias("regulatedactivities")
    )

    return df


def export_parquet_files(df, destination):
    print(f"Exporting as CQC provider parquet to {destination}")
    utils.write_to_parquet(df, destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file used as source input", required=True
    )
    parser.add_argument(
        "--provider_destination",
        help="A destination directory for outputting CQC Provider parquet file",
        required=True,
    )
    parser.add_argument(
        "--location_destination",
        help="A destination directory for outputting CQC Location parquet file",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.provider_destination, args.location_destination


if __name__ == "__main__":
    print("Spark job 'ingest_cqc_care_directory' starting...")
    print(f"Job parameters: {sys.argv}")

    source, provider_destination, location_destination = collect_arguments()
    main(source, provider_destination, location_destination)

    print("Spark job 'ingest_cqc_care_directory' complete")
