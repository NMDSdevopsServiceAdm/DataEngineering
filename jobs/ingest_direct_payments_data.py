import sys
import argparse

from schemas.direct_payment_data_schema import DIRECT_PAYMENTS_DATA
from utils import utils


def main(source, destination):
    df = utils.read_csv_with_defined_schema(source, DIRECT_PAYMENTS_DATA)

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

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'ingest_direct_payments_data' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'ingest_direct_payments_data' done")
