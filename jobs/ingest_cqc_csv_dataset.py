from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_timestamp
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
    provider_df = ...(df)

    print(f"Exporting Provider information as parquet to {provider_destination}")
    utils.write_to_parquet(provider_df, provider_destination)

    print("Create CQC provider parquet file")
    location_df = ...(df)

    print(f"Exporting Provider information as parquet to {location_destination}")
    utils.write_to_parquet(location_df, location_destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="A CSV file used as source input", required=True)
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
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

    return args.source, args.destination, args.delimiter


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination, delimiter = collect_arguments()
    main(source, destination, delimiter)

    print("Spark job 'ingest_ascwds_dataset' complete")
