import sys

import pyspark.sql
import pyspark.sql.functions as F

from utils import utils
from utils.ind_cqc_filled_posts_utils.filter_job_count.filter_job_count import (
    null_job_count_outliers,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_destination: str,
) -> pyspark.sql.DataFrame:
    print("Cleaning merged_ind_cqc dataset...")

    locations_df = utils.read_from_parquet(merged_ind_cqc_source)

    locations_df = replace_zero_beds_with_null(locations_df)
    locations_df = populate_missing_care_home_number_of_beds(locations_df)

    locations_df = null_job_count_outliers(locations_df)

    print(f"Exporting as parquet to {cleaned_ind_cqc_destination}")

    utils.write_to_parquet(
        locations_df,
        cleaned_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def replace_zero_beds_with_null(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df.replace(0, None, IndCqcColumns.number_of_beds)


def populate_missing_care_home_number_of_beds(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    care_home_df = filter_to_care_homes_with_known_beds(df)
    avg_beds_per_loc_df = average_beds_per_location(care_home_df)
    df = df.join(avg_beds_per_loc_df, IndCqcColumns.location_id, "left")
    df = replace_null_beds_with_average(df)
    return df


def filter_to_care_homes_with_known_beds(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.filter(F.col(IndCqcColumns.care_home) == "Y")
    df = df.filter(F.col(IndCqcColumns.number_of_beds).isNotNull())
    return df


def average_beds_per_location(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = df.groupBy(IndCqcColumns.location_id).agg(
        F.avg(IndCqcColumns.number_of_beds).alias("avg_beds")
    )
    df = df.withColumn("avg_beds", F.col("avg_beds").cast("int"))
    df = df.select(IndCqcColumns.location_id, "avg_beds")
    return df


def replace_null_beds_with_average(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        IndCqcColumns.number_of_beds,
        F.coalesce(IndCqcColumns.number_of_beds, "avg_beds"),
    )
    return df.drop("avg_beds")


if __name__ == "__main__":
    print("Spark job 'clean_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        merged_ind_cqc_source,
        cleaned_ind_cqc_destination,
    ) = utils.collect_arguments(
        (
            "--merged_ind_cqc_source",
            "Source s3 directory for merge_ind_cqc_data dataset",
        ),
        (
            "--cleaned_ind_cqc_destination",
            "A destination directory for outputting cleaned_ind_cqc_destination",
        ),
    )

    main(
        merged_ind_cqc_source,
        cleaned_ind_cqc_destination,
    )
