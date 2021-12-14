from utils import cqc_location_api as cqc
from utils import utils
from schemas.cqc_location_schema import LOCATION_SCHEMA
import argparse


def main(destination):
    print("Collecting all locations from API")
    spark = utils.get_spark()
    for paginated_locations in cqc.get_all_locations(stream=True):

        df = spark.createDataFrame(paginated_locations, LOCATION_SCHEMA)
        utils.write_to_parquet(df, destination, True)

    print(f"Finished! Files can be found in {destination}")


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--destination", help="A destination directory for outputting cqc locations", required=True)

    args, unknown = parser.parse_known_args()

    return args.destination


if __name__ == "__main__":
    destination = collect_arguments()
    main(destination)
