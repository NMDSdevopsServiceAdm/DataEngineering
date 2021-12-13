from utils import cqc_location_api as cqc
from utils import utils
from schemas.cqc_location_schema import LOCATION_SCHEMA


def main():
    print("Collecting all locations from API")
    spark = utils.get_spark()
    for paginated_locations in cqc.get_all_locations(stream=True):

        df = spark.createDataFrame(paginated_locations, LOCATION_SCHEMA)
        utils.write_to_parquet(df, append=True)

    print(f"Finished! Files can be found in {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
