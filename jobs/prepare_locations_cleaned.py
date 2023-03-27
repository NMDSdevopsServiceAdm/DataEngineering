import sys
from datetime import date

import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from utils import utils
from utils.prepare_locations_utils.filter_job_count.filter_job_count import (
    null_job_count_outliers,
)

COLUMNS_TO_IMPORT = [
    "locationid",
    "snapshot_date",
    "snapshot_day",
    "snapshot_month",
    "snapshot_year",
    "local_authority",
    "ons_region",
    "rural_urban_indicator.year_2011 AS rui_2011",
    "services_offered",
    "carehome",
    "primary_service_type",
    "cqc_sector",
    "registration_status",
    "number_of_beds",
    "people_directly_employed",
    "job_count_unfiltered_source",
    "job_count_unfiltered",
]


def main(
    prepared_locations_source: str,
    prepared_locations_cleaned_destination: str,
) -> pyspark.sql.DataFrame:
    spark = (
        SparkSession.builder.appName("sfc_data_engineering_prepared_locations_cleaned")
        .config("spark.sql.broadcastTimeout", 600)
        .getOrCreate()
    )
    print("Cleaning prepare_locations dataset...")

    locations_df = spark.read.parquet(prepared_locations_source).selectExpr(
        *COLUMNS_TO_IMPORT
    )

    locations_df = remove_unwanted_data(locations_df)

    locations_df = replace_zero_beds_with_null(locations_df)
    locations_df = populate_missing_carehome_number_of_beds(locations_df)

    locations_df = null_job_count_outliers(locations_df)

    locations_df = create_partition_keys_based_on_todays_date(locations_df)

    print(f"Exporting as parquet to {prepared_locations_cleaned_destination}")

    utils.write_to_parquet(
        locations_df,
        prepared_locations_cleaned_destination,
        append=True,
        partitionKeys=["run_year", "run_month", "run_day"],
    )


def remove_unwanted_data(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = df.where(F.col("cqc_sector") == "Independent")
    df = df.where(F.col("registration_status") == "Registered")
    return df


def replace_zero_beds_with_null(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df.replace(0, None, "number_of_beds")


def populate_missing_carehome_number_of_beds(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    care_home_df = filter_to_carehomes_with_known_beds(df)
    avg_beds_per_loc_df = average_beds_per_location(care_home_df)
    df = df.join(avg_beds_per_loc_df, "locationid", "left")
    df = replace_null_beds_with_average(df)
    return df


def filter_to_carehomes_with_known_beds(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.filter(F.col("carehome") == "Y")
    df = df.filter(F.col("number_of_beds").isNotNull())
    return df


def average_beds_per_location(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = df.groupBy("locationid").agg(F.avg("number_of_beds").alias("avg_beds"))
    df = df.withColumn("avg_beds", F.col("avg_beds").cast("int"))
    df = df.select("locationid", "avg_beds")
    return df


def replace_null_beds_with_average(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = df.withColumn("number_of_beds", F.coalesce("number_of_beds", "avg_beds"))
    return df.drop("avg_beds")


def create_partition_keys_based_on_todays_date(df):
    today = date.today()
    df = df.withColumn("run_year", F.lit(today.year))
    df = df.withColumn("run_month", F.lit(f"{today.month:0>2}"))
    df = df.withColumn("run_day", F.lit(f"{today.day:0>2}"))
    return df


if __name__ == "__main__":
    print("Spark job 'prepare_locations_cleaned' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        prepared_locations_source,
        prepared_locations_cleaned_destination,
    ) = utils.collect_arguments(
        (
            "--prepared_locations_source",
            "Source s3 directory for prepared_locations dataset",
        ),
        (
            "--prepared_locations_cleaned_destination",
            "A destination directory for outputting prepared_locations_cleaned, if not provided shall default to S3 todays date.",
        ),
    )

    main(
        prepared_locations_source,
        prepared_locations_cleaned_destination,
    )
