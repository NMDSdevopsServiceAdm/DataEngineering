import sys
import argparse

from schemas.spss_job_estimates_schema import SPSS_JOBS_ESTIMATES
from utils import utils

DEFAULT_DELIMITER = ","


def main(source, destination):
    df = utils.read_csv_with_defined_schema(source, SPSS_JOBS_ESTIMATES)
    utils.write_to_parquet(df, destination, False)


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

    args, _ = parser.parse_known_args()

    if args.delimiter:
        print(f"Utilising custom delimiter '{args.delimiter}'")

    return args.source, args.destination, args.delimiter


if __name__ == "__main__":
    print("Spark job 'csv_to_parquet' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination, delimiter = collect_arguments()
    main(source, destination)

    print("Spark job 'csv_to_parquet' done")
