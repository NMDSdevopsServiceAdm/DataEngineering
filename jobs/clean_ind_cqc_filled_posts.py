import sys

from pyspark.sql import (
    DataFrame,
    Window,
)
import pyspark.sql.functions as F

from utils import utils
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator import (
    calculate_ascwds_filled_posts,
)
from utils.ind_cqc_filled_posts_utils.filter_ascwds_filled_posts.filter_ascwds_filled_posts import (
    null_ascwds_filled_post_outliers,
)

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
average_number_of_beds: str = "avg_beds"


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_destination: str,
) -> DataFrame:
    print("Cleaning merged_ind_cqc dataset...")

    locations_df = utils.read_from_parquet(merged_ind_cqc_source)

    locations_df = replace_zero_beds_with_null(locations_df)
    locations_df = populate_missing_care_home_number_of_beds(locations_df)

    locations_df = calculate_ascwds_filled_posts(
        locations_df,
        IndCQC.total_staff_bounded,
        IndCQC.worker_records_bounded,
        IndCQC.ascwds_filled_posts,
        IndCQC.ascwds_filled_posts_source,
    )

    locations_df = null_ascwds_filled_post_outliers(locations_df)
    locations_df = clean_people_directly_employed(locations_df)

    locations_df = create_column_with_repeated_values_removed(
        locations_df,
        IndCQC.ascwds_filled_posts_clean,
        IndCQC.ascwds_filled_posts_dedup_clean,
    )
    locations_df = create_column_with_repeated_values_removed(
        locations_df,
        IndCQC.people_directly_employed_clean,
        IndCQC.people_directly_employed_clean_dedup,
    )

    print(f"Exporting as parquet to {cleaned_ind_cqc_destination}")

    utils.write_to_parquet(
        locations_df,
        cleaned_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def clean_people_directly_employed(df: DataFrame) -> DataFrame:

    return df


def replace_zero_people_with_one(df: DataFrame) -> DataFrame:
    return df


def replace_zero_beds_with_null(df: DataFrame) -> DataFrame:
    return df.replace(0, None, IndCQC.number_of_beds)


def populate_missing_care_home_number_of_beds(
    df: DataFrame,
) -> DataFrame:
    care_home_df = filter_to_care_homes_with_known_beds(df)
    avg_beds_per_loc_df = average_beds_per_location(care_home_df)
    df = df.join(avg_beds_per_loc_df, IndCQC.location_id, "left")
    df = replace_null_beds_with_average(df)
    return df


def filter_to_care_homes_with_known_beds(
    df: DataFrame,
) -> DataFrame:
    df = df.filter(F.col(IndCQC.care_home) == "Y")
    df = df.filter(F.col(IndCQC.number_of_beds).isNotNull())
    return df


def average_beds_per_location(df: DataFrame) -> DataFrame:
    df = df.groupBy(IndCQC.location_id).agg(
        F.avg(IndCQC.number_of_beds).alias(average_number_of_beds)
    )
    df = df.withColumn(
        average_number_of_beds, F.col(average_number_of_beds).cast("int")
    )
    df = df.select(IndCQC.location_id, average_number_of_beds)
    return df


def replace_null_beds_with_average(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        IndCQC.number_of_beds,
        F.coalesce(IndCQC.number_of_beds, average_number_of_beds),
    )
    return df.drop(average_number_of_beds)


def create_column_with_repeated_values_removed(
    df: DataFrame,
    column_to_clean: str,
    new_column_name: str = None,
) -> DataFrame:
    """
    Some data we have (such as ASCWDS) repeats data until it is changed. This function creates a new column which converts repeated
    values to nulls, so we only see newly submitted values once. This also happens as a result of joining the same datafile mulitple
    times as part of the align dates field.

    For each location, this function iterates over the dataframe in date order and compares the current column value to the
    previously submitted value. If the value differs from the previously submitted value then enter that value into the new column.
    Otherwise null the value in the new column as it is a previously submitted value which has been repeated.

    Args:
        df: The dataframe to use
        column_to_clean: The name of the column to convert
        new_column_name: (optional) If not provided, "_deduplicated" will be appended onto the original column name

    Returns:
        A DataFrame with an addional column with repeated values changed to nulls.
    """
    PREVIOUS_VALUE: str = "previous_value"

    if new_column_name is None:
        new_column_name = column_to_clean + "_deduplicated"

    w = Window.partitionBy(IndCQC.location_id).orderBy(IndCQC.cqc_location_import_date)

    df_with_previously_submitted_value = df.withColumn(
        PREVIOUS_VALUE, F.lag(column_to_clean).over(w)
    )

    df_without_repeated_values = df_with_previously_submitted_value.withColumn(
        new_column_name,
        F.when(
            (F.col(PREVIOUS_VALUE).isNull())
            | (F.col(column_to_clean) != F.col(PREVIOUS_VALUE)),
            F.col(column_to_clean),
        ).otherwise(None),
    )

    df_without_repeated_values = df_without_repeated_values.drop(PREVIOUS_VALUE)

    return df_without_repeated_values


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
