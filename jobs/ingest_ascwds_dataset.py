from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.functions import to_timestamp
from utils import utils
import sys
import pyspark
import argparse

DEFAULT_DELIMITER = ","


def main(source, destination, delimiter):
    print("Reading CSV from {source}")
    df = utils.read_csv(source, delimiter)
    print("Removing ASCWDS test accounts")
    df = filter_test_accounts(df)
    print("Formatting date fields")
    df = utils.format_date_fields(df)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(df, destination)


def filter_test_accounts(df):
    test_accounts = ["305", "307", "308", "309", "310", "2452", "28470", "26792", "31657"]
    df = df.filter(~df.orgid.isin(test_accounts))
    return df


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
