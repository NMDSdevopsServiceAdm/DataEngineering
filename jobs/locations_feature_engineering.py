import argparse
from attr import define

import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler

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


def define_features_list(regions):
    # fmt: off
    features = [
        'service_count','number_of_beds', 'dormancy_bool','service_1',
        'service_2','service_3','service_4','service_5','service_6','service_7',
        'service_8','service_9','service_10','service_11','service_12','service_13',
        'service_14','service_15','service_16','service_17','service_18','service_19',
        'service_20','service_21','service_22','service_23','service_24','service_25',
        'service_26','service_27','service_28','service_29','date_diff'
    ]
    # fmt: on
    return features + regions


def vectorize(locations_df, feature_list):
    vectorized_df = VectorAssembler(
        inputCols=feature_list, outputCol="features", handleInvalid="skip"
    ).transform(locations_df)
    return vectorized_df


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
