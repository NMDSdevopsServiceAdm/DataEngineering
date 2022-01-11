import argparse
from datetime import date

from schemas.cqc_location_schema import LOCATION_SCHEMA
from utils import cqc_api as cqc
from utils import utils


def main(destination):
    print("Collecting all providers from API")
    spark = utils.get_spark()
    for paginated_providers in cqc.get_all_objects(stream=True, object_type="providers", object_identifier="providerId"):

        # May need schema as second parameter
        df = spark.createDataFrame(paginated_providers)
        utils.write_to_parquet(df, destination, True)

    print(f"Finished! Files can be found in {destination}")


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--destination", help="A destination directory for outputting cqc providers, if not provided shall default to S3 todays date.", required=False)

    args, unknown = parser.parse_known_args()

    return args.destination


if __name__ == "__main__":
    destination = collect_arguments()
    if not destination:
        todays_date = date.today()
        destination = utils.generate_s3_dir_date_path(
            domain="CQC", dataset="providers-api", date=todays_date)

    main(destination)
