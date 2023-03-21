import sys

import pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from utils import utils
from utils.prepare_locations_utils.filter_job_count.filter_job_count import (
    null_job_count_outliers,
)


def main(
    prepared_locations_source: str, prepared_locations_cleaned_destination: str
) -> pyspark.sql.DataFrame:
    spark = (
        SparkSession.builder.appName("sfc_data_engineering_prepared_locations_cleaned")
        .config("spark.sql.broadcastTimeout", 600)
        .getOrCreate()
    )
    print("Cleaning prepare_locations dataset...")

    locations_df = spark.read.parquet(prepared_locations_source)

    locations_df = remove_unwanted_data(locations_df)

    locations_df = null_job_count_outliers(locations_df)

    locations_df = utils.create_partition_keys_based_on_todays_date(locations_df)

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


if __name__ == "__main__":
    print("Spark job 'estimate_job_counts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        prepared_locations_source,
        prepared_locations_cleaned_destination,
        JOB_RUN_ID,
        JOB_NAME,
    ) = utils.collect_arguments(
        (
            "--prepared_locations_source",
            "Source s3 directory for prepared_locations dataset",
        ),
        (
            "--prepared_locations_cleaned_destination",
            "A destination directory for outputting prepared_locations_cleaned, if not provided shall default to S3 todays date.",
        ),
        ("--JOB_RUN_ID", "The Glue job run id"),
        ("--JOB_NAME", "The Glue job name"),
    )

    main(
        prepared_locations_source,
        prepared_locations_cleaned_destination,
        JOB_RUN_ID,
        JOB_NAME,
    )
