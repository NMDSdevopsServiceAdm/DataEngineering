import sys
from datetime import date

import pyspark.sql
import pyspark.sql.functions as F

from utils import utils
from utils.ind_cqc_filled_posts_utils.filter_job_count.filter_job_count import (
    null_job_count_outliers,
)


COLUMNS_TO_IMPORT = [
    "locationid",
    "cqc_location_import_date",
    "day",
    "month",
    "year",
    "localAuthority as local_authority",
    # "ons_region",
    # "rural_urban_indicator AS rui_2011",
    # "services_offered",
    "carehome as care_home",
    "primary_service_type",
    "cqc_sector",
    "registrationStatus as registration_status",
    "numberOfBeds as number_of_beds",
    # "people_directly_employed",
    # "job_count_unfiltered_source",
    # "job_count_unfiltered",
]


def main(
    cqc_filled_posts_source: str,
    cqc_filled_posts_cleaned_destination: str,
) -> pyspark.sql.DataFrame:
    print("Cleaning cqc_filled_posts dataset...")

    locations_df = utils.read_from_parquet(cqc_filled_posts_source).selectExpr(
        *COLUMNS_TO_IMPORT
    )

    locations_df = remove_unwanted_data(locations_df)

    locations_df = replace_zero_beds_with_null(locations_df)
    locations_df = populate_missing_carehome_number_of_beds(locations_df)

    locations_df = null_job_count_outliers(locations_df)

    locations_df = create_partition_keys_based_on_todays_date(locations_df)

    print(f"Exporting as parquet to {cqc_filled_posts_cleaned_destination}")

    utils.write_to_parquet(
        locations_df,
        cqc_filled_posts_cleaned_destination,
        mode="append",
        partitionKeys=["import_date", "year", "month", "day"],
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
    df = df.withColumn("run_year", F.lit(f"{today.year}"))
    df = df.withColumn("run_month", F.lit(f"{today.month:0>2}"))
    df = df.withColumn("run_day", F.lit(f"{today.day:0>2}"))
    return df


if __name__ == "__main__":
    print("Spark job 'clean_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_filled_posts_source,
        cqc_filled_posts_cleaned_destination,
    ) = utils.collect_arguments(
        (
            "--ind_cqc_filled_posts_source",
            "Source s3 directory for ind_cqc_filled_posts dataset",
        ),
        (
            "--ind_cqc_filled_posts_cleaned_destination",
            "A destination directory for outputting ind_cqc_filled_posts_cleaned",
        ),
    )

    main(
        cqc_filled_posts_source,
        cqc_filled_posts_cleaned_destination,
    )
