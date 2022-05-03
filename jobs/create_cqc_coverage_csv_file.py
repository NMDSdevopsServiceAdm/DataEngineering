import argparse
from utils import utils
from jobs import prepare_locations
from pyspark.sql.functions import to_date
from pyspark.sql import functions as f

required_cqc_location_fields = [
    "locationid",
    "providerid",
    "name",
    "postalcode",
    "type",
    "registrationstatus",
    "localauthority",
    "region",
]

required_cqc_provider_fields = [
    "providerid",
    "name",
]

required_ascwds_fields = [
    "locationid",
    "establishmentid",
    "orgid",
    "isparent",
    "import_date",
    "mupddate",
    "lapermission",
]

output_fields = [
    "locationid",
    "name",
    "postalcode",
    "providerid",
    "providername",
    "region",
    "localauthority",
    "location_in_ASCWDS",
    "lapermission",
]


def main(workplace_source, cqc_location_source, cqc_provider_source, destination):
    cqc_location_df = get_cqc_locations_df(cqc_location_source)
    cqc_provider_df = get_cqc_providers_df(cqc_provider_source)

    output_df = cqc_location_df.join(cqc_provider_df, "providerid", "left")

    ascwds_workplace_df = get_ascwds_workplace_df(workplace_source)
    ascwds_workplace_df = prepare_locations.purge_workplaces(ascwds_workplace_df)

    output_df = output_df.join(ascwds_workplace_df, "locationid", "left")

    output_df = output_df.select(output_fields).sort("localauthority", "providername", "name")

    output_df = relabel_permission_col(output_df)

    # print regional coverage
    output_df.groupBy("region").agg(
        f.countDistinct("locationid").alias("CQC_Locations"),
        f.countDistinct("locationid_ASCWDS").alias("ASCWDS_Locations"),
        round(f.countDistinct("locationid_ASCWDS") / f.countDistinct("locationid"), 2).alias("CQC_Coverage"),
    ).show()

    print(f"Exporting as csv to {destination}")
    if destination:
        output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(destination)

    else:
        return output_df


def get_cqc_locations_df(dataset_locations_api_source):
    spark = utils.get_spark()
    print(f"Reading CQC locations parquet from {dataset_locations_api_source}")
    cqc_locations_df = spark.read.parquet(dataset_locations_api_source).select(required_cqc_location_fields).distinct()

    cqc_locations_df = cqc_locations_df.filter(
        (cqc_locations_df.registrationstatus == "Registered") & (cqc_locations_df.type == "Social Care Org")
    )

    return cqc_locations_df


def get_cqc_providers_df(dataset_providers_api_source):
    spark = utils.get_spark()
    print(f"Reading CQC providers parquet from {dataset_providers_api_source}")
    cqc_providers_df = spark.read.parquet(dataset_providers_api_source).select(required_cqc_provider_fields).distinct()
    cqc_providers_df = cqc_providers_df.withColumnRenamed("name", "provider_name")

    return cqc_providers_df


def get_ascwds_workplace_df(dataset_workplace_source):
    spark = utils.get_spark()
    print(f"Reading ASCWDS workplace parquet from {dataset_workplace_source}")
    ascwds_workplace_df = spark.read.parquet(dataset_workplace_source).select(required_ascwds_fields).distinct()

    ascwds_workplace_df = ascwds_workplace_df.withColumn("locationid_ASCWDS", ascwds_workplace_df.locationid)

    return ascwds_workplace_df


def relabel_permission_col(input_df):
    input_df = input_df.withColumn(
        "lapermission",
        f.when(input_df.lapermission == -1, "Not recorded")
        .when(input_df.lapermission == 0, "No")
        .when(input_df.lapermission == 1, "Yes"),
    )
    input_df.show()
    return input_df


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--workplace_source",
        help="Source s3 directory for ASCWDS workplace dataset",
        required=True,
    )
    parser.add_argument(
        "--cqc_location_source",
        help="Source s3 directory for CQC locations api dataset",
        required=True,
    )
    parser.add_argument(
        "--cqc_provider_source",
        help="Source s3 directory for CQC providers api dataset",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting cqc locations, if not provided shall default to S3 todays date.",
        required=True,
    )

    args, unknown = parser.parse_known_args()

    return (
        args.workplace_source,
        args.cqc_location_source,
        args.cqc_provider_source,
        args.destination,
    )


if __name__ == "__main__":
    (
        workplace_source,
        cqc_location_source,
        cqc_provider_source,
        destination,
    ) = collect_arguments()
    main(workplace_source, cqc_location_source, cqc_provider_source, destination)
