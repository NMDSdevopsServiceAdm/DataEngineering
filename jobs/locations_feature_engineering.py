import argparse
import re
import sys

import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler

from utils import utils, feature_engineering_dictionaries


def main(prepared_locations_source, destination=None):
    spark = utils.get_spark()

    locations_df = spark.read.option("basePath", prepared_locations_source).parquet(
        prepared_locations_source
    )
    max_snapshot = utils.get_max_snapshot_partitions(destination)
    locations_df = filter_records_since_snapshot_date(locations_df, max_snapshot)

    locations_df = locations_df.withColumn(
        "service_count", F.size(locations_df.services_offered)
    )

    locations_df = days_diff_from_latest_snapshot(locations_df)
    locations_df = explode_array_column_using_dictionary(
        locations_df,
        "services_offered",
        feature_engineering_dictionaries.SERVICES_LOOKUP,
    )
    locations_df = explode_string_column_using_dictionary(
        locations_df,
        "rural_urban_indicator.2011",
        feature_engineering_dictionaries.RURAL_URBAN_INDICATOR_LOOKUP,
    )

    locations_df, regions = explode_column(locations_df, "ons_region")
    locations_df, local_authorities = explode_column(locations_df, "local_authority")
    locations_df, cqc_sectors = explode_column(locations_df, "cqc_sector")

    care_home_feature_list = define_care_home_features_list(
        regions, local_authorities, cqc_sectors
    )
    non_residential_inc_pir_feature_list = define_non_res_inc_pir_features_list(
        regions, local_authorities, cqc_sectors
    )

    locations_df = vectorize_features(
        locations_df, care_home_feature_list, "care_home_features"
    )
    locations_df = vectorize_features(
        locations_df,
        non_residential_inc_pir_feature_list,
        "non_residential_inc_pir_features",
    )

    locations_df = locations_df.select(
        "locationid",
        "snapshot_date",
        "ons_region",
        "number_of_beds",
        "people_directly_employed",
        "snapshot_year",
        "snapshot_month",
        "snapshot_day",
        "carehome",
        "care_home_features",
        "non_residential_inc_pir_features",
    )

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(
            locations_df,
            destination,
            append=True,
            partitionKeys=["snapshot_year", "snapshot_month", "snapshot_day"],
        )
    return locations_df


def filter_records_since_snapshot_date(locations_df, max_snapshot):
    if not max_snapshot:
        return locations_df
    max_snapshot_date = f"{max_snapshot[0]}{max_snapshot[1]:0>2}{max_snapshot[2]:0>2}"
    print(f"max snapshot previously processed: {max_snapshot_date}")
    locations_df = locations_df.withColumn(
        "snapshot_full_date",
        F.concat(
            locations_df.snapshot_year.cast("string"),
            F.lpad(locations_df.snapshot_month.cast("string"), 2, "0"),
            F.lpad(locations_df.snapshot_day.cast("string"), 2, "0"),
        ),
    )
    locations_df = locations_df.where(
        locations_df["snapshot_full_date"] > max_snapshot_date
    )
    locations_df.drop("snapshot_full_date")
    return locations_df


def format_column_name(column):
    non_lower_alpha = re.compile("[^a-z_]")
    formatted_column_name = column.replace(" ", "_").lower()
    return re.sub(non_lower_alpha, "", formatted_column_name)


def explode_column(locations_df, column_name):
    distinct_category_rows = locations_df.select(column_name).distinct().collect()
    categories = []
    for row in distinct_category_rows:
        if row[column_name]:
            formatted_column_name = format_column_name(row[column_name])

            locations_df = locations_df.withColumn(
                formatted_column_name,
                F.when(locations_df[column_name] == row[column_name], 1).otherwise(0),
            )
        else:
            formatted_column_name = "unspecified"

            locations_df = locations_df.withColumn(
                formatted_column_name,
                F.when(locations_df[column_name].isNull(), 1).otherwise(0),
            )

        categories.append(formatted_column_name)

    return locations_df, categories


def explode_array_column_using_dictionary(locations_df, column_name, dictionary):
    for name, description in dictionary.items():

        locations_df = locations_df.withColumn(
            name,
            F.when(
                F.array_contains(locations_df[column_name], description), 1
            ).otherwise(0),
        )
    return locations_df


def explode_string_column_using_dictionary(locations_df, column_name, dictionary):
    for name, description in dictionary.items():

        locations_df = locations_df.withColumn(
            name,
            F.when(locations_df[column_name] == description, 1).otherwise(0),
        )
    return locations_df


def define_care_home_features_list(regions, local_authorites, cqc_sectors):
    services = list(feature_engineering_dictionaries.SERVICES_LOOKUP.keys())
    rural_urban_indicators = list(
        feature_engineering_dictionaries.RURAL_URBAN_INDICATOR_LOOKUP.keys()
    )
    features = ["service_count", "number_of_beds", "date_diff"]
    return (
        features
        + regions
        + cqc_sectors
        + rural_urban_indicators
        + local_authorites
        + services
    )


def define_non_res_inc_pir_features_list(regions, local_authorites, cqc_sectors):
    services = list(feature_engineering_dictionaries.SERVICES_LOOKUP.keys())
    rural_urban_indicators = list(
        feature_engineering_dictionaries.RURAL_URBAN_INDICATOR_LOOKUP.keys()
    )
    features = ["service_count", "people_directly_employed", "date_diff"]
    return (
        features
        + regions
        + cqc_sectors
        + rural_urban_indicators
        + local_authorites
        + services
    )


def vectorize_features(locations_df, feature_list, column_name):
    vectorized_df = VectorAssembler(
        inputCols=feature_list, outputCol=column_name, handleInvalid="skip"
    ).transform(locations_df)
    return vectorized_df


def days_diff_from_latest_snapshot(locations_df):
    max_snapshot_date = utils.get_max_snapshot_date(locations_df)
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
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting locations dataset after feature engineering and vectorizing",
        required=True,
    )
    args, _ = parser.parse_known_args()

    return args.prepared_locations_source, args.destination


if __name__ == "__main__":
    print("Spark job 'locations_feature_engineering' starting...")
    print(f"Job parameters: {sys.argv}")

    (prepared_locations_source, destination) = collect_arguments()

    main(prepared_locations_source, destination)

    print("Spark job 'locations_feature_engineering' complete")
