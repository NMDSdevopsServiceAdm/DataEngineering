import argparse
from datetime import date

from utils import cqc_api as cqc
from utils import utils
from schemas.cqc_location_schema import LOCATION_SCHEMA


def main(destination):
    print("Collecting all locations from API")
    spark = utils.get_spark()
    df = None
    for paginated_locations in cqc.get_all_objects(
        stream=True, object_type="locations", object_identifier="locationId"
    ):
        locations_df = spark.createDataFrame(paginated_locations, LOCATION_SCHEMA)
        if df:
            df = df.union(locations_df)
        else:
            df = locations_df

    df = df.dropDuplicates(["locationId"])
    utils.write_to_parquet(df, destination, True)

    print(f"Finished! Files can be found in {destination}")


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--destination_prefix",
        help="A destination bucket name in the format of s3://<bucket_name>/",
        required=False,
    )

    args, _ = parser.parse_known_args()

    return args.destination_prefix


if __name__ == "__main__":
    destination_prefix = collect_arguments()
    todays_date = date.today()
    destination = utils.generate_s3_main_datasets_dir_date_path(
        destination_prefix=destination_prefix,
        domain="CQC",
        dataset="locations-api",
        date=todays_date,
    )

    print(destination)

    main(destination)
