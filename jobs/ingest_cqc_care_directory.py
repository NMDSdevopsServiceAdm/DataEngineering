from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, collect_set
from utils import utils
import sys
import pyspark
import argparse


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
    provider_df = provider_df.withColumn("organisationType", lit("Provider"))
    provider_df = provider_df.withColumn("registrationstatus", lit("Registered"))

    print(f"Exporting Provider information as parquet to {provider_destination}")
    if provider_destination:
        utils.write_to_parquet(provider_df, provider_destination)
    else:
        return_datasets.append(provider_df)

    print("Create CQC provider parquet file")
    location_df = df

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

    return prov_info_df


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
