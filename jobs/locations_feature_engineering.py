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

    locations_df = days_diff_from_latest_snapshot(locations_df)
    locations_df = explode_services(locations_df)
    locations_df, regions = explode_regions(locations_df)

    return locations_df


def explode_regions(locations_df):
    distinct_region_rows = locations_df.select("region").distinct().collect()
    regions = []
    for row in distinct_region_rows:
        region_column_name = row.region.replace(" ", "_").lower()
        regions.append(region_column_name)

        locations_df = locations_df.withColumn(
            region_column_name,
            F.when(locations_df.region == row.region, 1).otherwise(0),
        )
    return locations_df, regions


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


def days_diff_from_latest_snapshot(locations_df):
    max_snapshot_date = (
        locations_df.select(F.max("snapshot_date").alias("max")).first().max
    )
    locations_df = locations_df.withColumn(
        "date_diff", F.datediff(F.lit(max_snapshot_date), locations_df.snapshot_date)
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
