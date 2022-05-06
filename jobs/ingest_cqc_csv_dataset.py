from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import col, collect_set
from utils import utils
import sys
import pyspark
import argparse

DEFAULT_DELIMITER = ","


def main(source, provider_destination, location_destination, delimiter):
    print("Reading CSV from {source}")
    df = utils.read_csv(source, delimiter)

    print("Formatting date fields")
    df = utils.format_date_fields(df)

    print("Create CQC provider parquet file")
    provider_df = unique_providers_with_locations(df)

    print(f"Exporting Provider information as parquet to {provider_destination}")
    utils.write_to_parquet(provider_df, provider_destination)

    # print("Create CQC provider parquet file")
    # location_df = ...(df)

    # print(f"Exporting Provider information as parquet to {location_destination}")
    # utils.write_to_parquet(location_df, location_destination)


def unique_providers_with_locations(df):
    locations_at_prov_df = df.select("providerid", "locationid")
    locations_at_prov_df = (
        locations_at_prov_df.groupby("providerid")
        .agg(collect_set("locationid"))
        .withColumnRenamed("collect_set(locationid)", "locationids")
    )

    return locations_at_prov_df


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="A CSV file used as source input", required=True)
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
    parser.add_argument(
        "--delimiter",
        help="Specify a custom field delimiter",
        required=False,
        default=DEFAULT_DELIMITER,
    )

    args, unknown = parser.parse_known_args()

    if args.delimiter:
        print(f"Utilising custom delimiter '{args.delimiter}'")

    return args.source, args.provider_destination, args.location_destination, args.delimiter


if __name__ == "__main__":
    print("Spark job 'ingest_cqc_csv_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, provider_destination, location_destination, delimiter = collect_arguments()
    main(source, provider_destination, location_destination, delimiter)

    print("Spark job 'ingest_cqc_csv_dataset' complete")
