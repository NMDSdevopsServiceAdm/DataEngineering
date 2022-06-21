import argparse

from utils import utils
import pyspark.sql.functions as F


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
    service_columns = services_lookup().items()
    for column_name, service_description in service_columns:

        locations_df = locations_df.withColumn(
            column_name,
            F.when(
                F.array_contains(locations_df.services_offered, service_description), 1
            ).otherwise(0),
        )
    return locations_df


def services_lookup():
    return {
        "service_1": "Care home service with nursing",
        "service_2": "Care home service without nursing",
        "service_3": "Community based services for people who misuse substances",
        "service_4": "Hospice services",
        "service_5": "Domiciliary care service",
        "service_6": "Remote clinical advice service",
        "service_7": "Acute services without overnight beds / listed acute services with or without overnight beds",
        "service_8": "Specialist college service",
        "service_9": "Ambulance service",
        "service_10": "Extra Care housing services",
        "service_11": "Urgent care services",
        "service_12": "Supported living service",
        "service_13": "Prison Healthcare Services",
        "service_14": "Community based services for people with mental health needs",
        "service_15": "Community healthcare service",
        "service_16": "Community based services for people with a learning disability",
        "service_17": "Community health care services - Nurses Agency only",
        "service_18": "Dental service",
        "service_19": "Mobile doctors service",
        "service_20": "Long term conditions services",
        "service_21": "Doctors consultation service",
        "service_22": "Shared Lives",
        "service_23": "Acute services with overnight beds",
        "service_24": "Diagnostic and/or screening service",
        "service_25": "Residential substance misuse treatment and/or rehabilitation service",
        "service_26": "Rehabilitation services",
        "service_27": "Doctors treatment service",
        "service_28": "Hospice services at home",
        "service_29": "Hospital services for people with mental health needs, learning disabilities and problems with substance misuse",
    }


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
