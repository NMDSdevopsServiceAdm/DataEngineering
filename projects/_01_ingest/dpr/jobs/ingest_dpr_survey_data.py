import sys
import argparse

from projects._01_ingest.utils.utils import ingest_utils
from schemas.direct_payment_data_schema import SURVEY_DATA
from utils import utils


def main(
    survey_data_source,
    survey_data_destination,
):
    survey_df = ingest_utils.read_csv_with_defined_schema(
        survey_data_source, SURVEY_DATA
    )

    utils.write_to_parquet(survey_df, survey_data_destination)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--survey_data_source",
        help="A CSV file used as source input for the IE/PA survey data",
        required=True,
    )
    parser.add_argument(
        "--survey_data_destination",
        help="A destination directory for outputting survey data parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return (
        args.survey_data_source,
        args.survey_data_destination,
    )


if __name__ == "__main__":
    print("Spark job 'ingest_direct_payments_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        survey_data_source,
        survey_data_destination,
    ) = collect_arguments()
    main(
        survey_data_source,
        survey_data_destination,
    )

    print("Spark job 'ingest_direct_payments_data' done")
