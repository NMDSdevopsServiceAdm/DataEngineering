import argparse

import pyspark.sql.functions as F

from utils import utils, feature_engineering_dictionaries


def main(prepared_locations_source):
    spark = utils.get_spark()

    locations_df = spark.read.option("basePath", prepared_locations_source).parquet(
        prepared_locations_source
    )

    locations_df = locations_df.withColumn(
        "service_count", F.size(locations_df.services_offered)
    )

    locations_df = explode_services(locations_df)

    return locations_df


def explode_services(locations_df):
    services_lookup = feature_engineering_dictionaries.SERVICES_LOOKUP
    for column_name, service_description in services_lookup.items():

        locations_df = locations_df.withColumn(
            column_name,
            F.when(
                F.array_contains(locations_df.services_offered, service_description), 1
            ).otherwise(0),
        )
    return locations_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--prepared_locations_source",
        help="Source S3 directory for data engineering prepared locations dataset",
        required=True,
    )
    args, _ = parser.parse_known_args()

    return args.prepared_locations_source


if __name__ == "__main__":
    (prepared_locations_source) = collect_arguments()

    main(prepared_locations_source)
