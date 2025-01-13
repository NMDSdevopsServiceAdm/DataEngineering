import sys
from typing import Optional

from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import CareHome
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator import (
    calculate_ascwds_filled_posts,
)
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers import (
    clean_ascwds_filled_post_outliers,
)
from utils.ind_cqc_filled_posts_utils.ascwds_pir_utils.blend_ascwds_pir import (
    blend_pir_and_ascwds_when_ascwds_out_of_date,
)


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
average_number_of_beds: str = "avg_beds"


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_destination: str,
) -> DataFrame:
    print("Cleaning merged_ind_cqc dataset...")

    locations_df = utils.read_from_parquet(merged_ind_cqc_source)

    locations_df = cUtils.reduce_dataset_to_earliest_file_per_month(locations_df)

    locations_df = remove_duplicate_cqc_care_homes(locations_df)

    locations_df = replace_zero_beds_with_null(locations_df)
    locations_df = populate_missing_care_home_number_of_beds(locations_df)

    locations_df = calculate_ascwds_filled_posts(
        locations_df,
        IndCQC.total_staff_bounded,
        IndCQC.worker_records_bounded,
        IndCQC.ascwds_filled_posts,
        IndCQC.ascwds_filled_posts_source,
    )

    locations_df = create_column_with_repeated_values_removed(
        locations_df,
        IndCQC.ascwds_filled_posts,
        IndCQC.ascwds_filled_posts_dedup,
    )

    locations_df = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_df, IndCQC.ascwds_filled_posts_dedup
    )

    locations_df = clean_ascwds_filled_post_outliers(locations_df)

    locations_df = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_df, IndCQC.ascwds_filled_posts_dedup_clean
    )

    locations_df = create_column_with_repeated_values_removed(
        locations_df,
        IndCQC.people_directly_employed,
        IndCQC.people_directly_employed_dedup,
    )
    locations_df = blend_pir_and_ascwds_when_ascwds_out_of_date(locations_df)

    print(f"Exporting as parquet to {cleaned_ind_cqc_destination}")

    utils.write_to_parquet(
        locations_df,
        cleaned_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def remove_duplicate_cqc_care_homes(df: DataFrame) -> DataFrame:
    """
    Removes cqc locations with dual registration and ensures no loss of ascwds data.

    This function removes one instance of cqc care home locations with dual registration. Duplicates
    are identified using cqc_location_import_date, name, postcode, and carehome. Any ASCWDS data in either
    location is shared to the other and then the location with the newer registration date is removed.

    Args:
        df (DataFrame): A dataframe containing cqc location data and ascwds data

    Returns:
        DataFrame: A dataframe with dual regestrations deduplicated and ascwds data retained.
    """
    duplicate_columns = [
        IndCQC.cqc_location_import_date,
        IndCQC.name,
        IndCQC.postcode,
        IndCQC.care_home,
    ]
    distinguishing_column = IndCQC.imputed_registration_date
    df = copy_ascwds_data_across_duplicate_rows(df, duplicate_columns)
    df = deduplicate_care_homes(df, duplicate_columns, distinguishing_column)
    return df


def deduplicate_care_homes(
    df: DataFrame, duplicate_columns: list, distinguishing_column: str
) -> DataFrame:
    """
    Removes cqc locations with dual registration.

    This function removes the more recently registered instance of cqc care home locations with dual registration.

    Args:
        df (DataFrame): A dataframe containing cqc location data and ascwds data.
        duplicate_columns (list): A list of column names to identify duplicates.
        distinguishing_column (str): The name of the column which will decide which of the duplicates to keep.

    Returns:
        DataFrame: A dataframe with dual regestrations deduplicated.
    """
    temp_col = "row_number"
    df = df.withColumn(
        temp_col,
        F.row_number().over(
            Window.partitionBy(duplicate_columns).orderBy(distinguishing_column)
        ),
    )
    df = df.where(
        (
            (F.col(IndCQC.care_home) == CareHome.care_home) & (F.col(temp_col) == 1)
            | ((F.col(IndCQC.care_home) == CareHome.not_care_home))
        )
    ).drop(temp_col)
    return df


def copy_ascwds_data_across_duplicate_rows(
    df: DataFrame, duplicate_columns: list
) -> DataFrame:
    """
    Copies total_staff_bounded and worker_records_bounded across duplicate rows.

    Args:
        df (DataFrame): A dataframe containing cqc location data and ascwds data.
        duplicate_columns (list): A list of column names to identify duplicates.

    Returns:
        DataFrame: A dataframe with total_staff_bounded and worker_records_bounded copied across duplicate rows.
    """
    window = (
        Window.partitionBy(duplicate_columns)
        .orderBy(IndCQC.imputed_registration_date)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    columns_to_copy = [IndCQC.total_staff_bounded, IndCQC.worker_records_bounded]
    functions_to_run = [F.first, F.last]
    for column in columns_to_copy:
        for function in functions_to_run:
            df = df.withColumn(
                column,
                F.when(
                    (df[IndCQC.care_home] == CareHome.care_home)
                    & (df[column].isNull()),
                    function(column).over(window),
                ).otherwise(F.col(column)),
            )
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
    df = df.filter(F.col(IndCQC.care_home) == CareHome.care_home)
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
    new_column_name: Optional[str] = None,
) -> DataFrame:
    """
    Some data we have (such as ASCWDS) repeats data until it is changed. This function creates a new column which converts repeated
    values to nulls, so we only see newly submitted values once. This also happens as a result of joining the same datafile mulitple
    times as part of the align dates field.

    For each location, this function iterates over the dataframe in date order and compares the current column value to the
    previously submitted value. If the value differs from the previously submitted value then enter that value into the new column.
    Otherwise null the value in the new column as it is a previously submitted value which has been repeated.

    Args:
        df (DataFrame): The dataframe to use
        column_to_clean (str): The name of the column to convert
        new_column_name (Optional [str]): If not provided, "_deduplicated" will be appended onto the original column name

    Returns:
        DataFrame: A DataFrame with an addional column with repeated values changed to nulls.
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
