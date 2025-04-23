import sys
import argparse

from _01_ingest.utils.utils import ingest_utils
from utils import utils

DEFAULT_DELIMITER = ","


def main(source, destination, delimiter):
    df = ingest_utils.read_csv(source, delimiter)
    utils.write_to_parquet(df, destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file used as source input", required=True
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )
    parser.add_argument(
        "--delimiter",
        help="Specify a custom field delimiter",
        required=False,
        default=",",
    )

    args, _ = parser.parse_known_args()

    if args.delimiter:
        print(f"Utilising custom delimiter '{args.delimiter}'")

    return args.source, args.destination, args.delimiter


if __name__ == "__main__":
    print("Spark job 'csv_to_parquet' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination, delimiter = collect_arguments()
    main(source, destination, delimiter)

    print("Spark job 'csv_to_parquet' done")
