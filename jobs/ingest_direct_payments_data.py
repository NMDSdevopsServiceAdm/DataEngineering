import sys
import argparse

from schemas.direct_payment_data_schema import DIRECT_PAYMENTS_DATA
from utils import utils


def main(external_data_source, survey_data_source, external_data_destination, survey_data_destination):
    df = utils.read_csv_with_defined_schema(external_data_source, DIRECT_PAYMENTS_DATA)

    utils.write_to_parquet(df, external_data_destination, False)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--external_data_source", help="A CSV file used as source input for external data", required=True
    )
    parser.add_argument(
        "--survey_data_source", help="A CSV file used as source input for the IE/PA survey data", required=True
    )
    parser.add_argument(
        "--external_data_destination",
        help="A destination directory for outputting external data parquet files",
        required=True,
    )
    parser.add_argument(
        "--survey_data_destination",
        help="A destination directory for outputting survey data parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.external_data_source, args.survey_data_source, args.external_data_destination, args.survey_data_destination


if __name__ == "__main__":
    print("Spark job 'ingest_direct_payments_data' starting...")
    print(f"Job parameters: {sys.argv}")

    external_data_source, survey_data_source, external_data_destination, survey_data_destination = collect_arguments()
    main(external_data_source, survey_data_source, external_data_destination, survey_data_destination)

    print("Spark job 'ingest_direct_payments_data' done")
