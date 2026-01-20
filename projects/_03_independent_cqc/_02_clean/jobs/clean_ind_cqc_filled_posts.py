import os
import sys
from dataclasses import dataclass
from typing import Optional

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import utils.cleaning_utils as cUtils
from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.ascwds_filled_posts_calculator import (
    calculate_ascwds_filled_posts,
)
from projects._03_independent_cqc._02_clean.utils.clean_ascwds_filled_post_outliers.clean_ascwds_filled_post_outliers import (
    clean_ascwds_filled_post_outliers,
)
from projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_care_home_outliers import (
    clean_capacity_tracker_care_home_outliers,
)
from projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_non_res_outliers import (
    clean_capacity_tracker_non_res_outliers,
)
from projects._03_independent_cqc._02_clean.utils.forward_fill_latest_known_value import (
    forward_fill_latest_known_value,
)
from projects._03_independent_cqc._02_clean.utils.utils import (
    create_column_with_repeated_values_removed,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome, Dormancy

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
average_number_of_beds: str = "avg_beds"


@dataclass
class NumericalValues:
    number_of_days_to_forward_fill = 65  # Note: using 65 as a proxy for 2 months


def main(
    merged_ind_cqc_source: str,
    cleaned_ind_cqc_destination: str,
) -> DataFrame:
    print("Cleaning merged_ind_cqc dataset...")

    locations_df = utils.read_from_parquet(merged_ind_cqc_source)

    locations_df = cUtils.reduce_dataset_to_earliest_file_per_month(locations_df)

    locations_df = calculate_time_registered_for(locations_df)
    locations_df = calculate_time_since_dormant(locations_df)

    locations_df = remove_dual_registration_cqc_care_homes(locations_df)

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
    locations_df = create_column_with_repeated_values_removed(
        locations_df,
        IndCQC.pir_people_directly_employed_cleaned,
        IndCQC.pir_people_directly_employed_dedup,
    )

    locations_df = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_df,
        IndCQC.ascwds_filled_posts_dedup,
        IndCQC.filled_posts_per_bed_ratio,
    )

    locations_df = cUtils.create_banded_bed_count_column(
        locations_df,
        IndCQC.number_of_beds_banded,
        [0, 1, 3, 5, 10, 15, 20, 25, 50, float("Inf")],
    )

    locations_df = clean_ascwds_filled_post_outliers(locations_df)

    locations_df = forward_fill_latest_known_value(
        locations_df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        NumericalValues.number_of_days_to_forward_fill,
    )

    locations_df = forward_fill_latest_known_value(
        locations_df,
        IndCQC.pir_people_directly_employed_dedup,
        NumericalValues.number_of_days_to_forward_fill,
    )

    locations_df = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.filled_posts_per_bed_ratio,
    )

    locations_df = cUtils.calculate_filled_posts_per_bed_ratio(
        locations_df,
        IndCQC.ct_care_home_total_employed,
        IndCQC.ct_care_home_posts_per_bed_ratio,
    )

    locations_df = clean_capacity_tracker_care_home_outliers(locations_df)
    locations_df = clean_capacity_tracker_non_res_outliers(locations_df)

    locations_df = calculate_care_home_status_count(locations_df)

    print(f"Exporting as parquet to {cleaned_ind_cqc_destination}")

    utils.write_to_parquet(
        locations_df,
        cleaned_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def remove_dual_registration_cqc_care_homes(df: DataFrame) -> DataFrame:
    """
    Removes cqc care home locations with dual registration and ensures no loss of ascwds data.

    This function removes one instance of cqc care home locations with dual registration. These
    are identified using cqc_location_import_date, name, postcode, and carehome. Any ASCWDS data in either
    location is shared to the other and then the location with the newer registration date is removed.

    The CQC locations dataset includes instances of 'dual registration', where two providers have evidenced to
    CQC that they are both responsible for managing the regulated activities at a single location.
    In this data, these instances appear as two separate lines, with different Location IDs, but with the same
    names and addresses of services. To understand care provision in England accurately, one of these 'dual registered'
    location pairs should be removed.

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
    distinguishing_columns = [IndCQC.imputed_registration_date, IndCQC.location_id]
    df = copy_ascwds_data_across_duplicate_rows(df, duplicate_columns)
    df = deduplicate_care_homes(df, duplicate_columns, distinguishing_columns)
    return df


def deduplicate_care_homes(
    df: DataFrame, duplicate_columns: list[str], distinguishing_columns: list[str]
) -> DataFrame:
    """
    Removes cqc locations with dual registration.

    This function removes the more recently registered instance of cqc care home locations with dual registration.

    Args:
        df (DataFrame): A dataframe containing cqc location data and ascwds data.
        duplicate_columns (list[str]): A list of column names to identify duplicates.
        distinguishing_columns (list[str]): A list of the columns which will decide which of the duplicates to keep.

    Returns:
        DataFrame: A dataframe with dual regestrations deduplicated.
    """
    temp_col = "row_number"
    df = df.withColumn(
        temp_col,
        F.row_number().over(
            Window.partitionBy(duplicate_columns).orderBy(distinguishing_columns)
        ),
    )
    df = df.where(
        (
            (F.col(IndCQC.care_home) == CareHome.care_home) & (F.col(temp_col) == 1)
            | (F.col(IndCQC.care_home) == CareHome.not_care_home)
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
    window = Window.partitionBy(duplicate_columns)

    df = df.withColumns(
        {
            IndCQC.total_staff_bounded: F.when(
                df[IndCQC.care_home] == CareHome.care_home,
                F.coalesce(
                    F.col(IndCQC.total_staff_bounded),
                    F.max(IndCQC.total_staff_bounded).over(window),
                ),
            ).otherwise(F.col(IndCQC.total_staff_bounded)),
            IndCQC.worker_records_bounded: F.when(
                df[IndCQC.care_home] == CareHome.care_home,
                F.coalesce(
                    F.col(IndCQC.worker_records_bounded),
                    F.max(IndCQC.worker_records_bounded).over(window),
                ),
            ).otherwise(F.col(IndCQC.worker_records_bounded)),
        },
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


def calculate_time_registered_for(df: DataFrame) -> DataFrame:
    """
    Adds a new column called time_registered which is the number of months the location has been registered with CQC for (rounded up).

    This function adds a new integer column to the given data frame which represents the number of months (rounded up) between the
    imputed registration date and the cqc location import date.

    Args:
        df (DataFrame): A dataframe containing the columns: imputed_registration_date and cqc_location_import_date.

    Returns:
        DataFrame: A dataframe with the new time_registered column added.
    """
    df = df.withColumn(
        IndCQC.time_registered,
        F.floor(
            F.months_between(
                F.col(IndCQC.cqc_location_import_date),
                F.col(IndCQC.imputed_registration_date),
            )
        )
        + 1,
    )

    return df


def calculate_time_since_dormant(df: DataFrame) -> DataFrame:
    """
    Adds a column to show the number of months since the location was last dormant.

    This function calculates the number of months since the last time a location was marked as dormant.
    It uses a window function to track the most recent date when dormancy was marked as "Y" and calculates
    the number of months since that date for each location.

    'time_since_dormant' values before the first instance of dormancy are null.
    If the location has never been dormant then 'time_since_dormant' is null.

    Args:
        df (DataFrame): A dataframe with columns: cqc_location_import_date, dormancy, and location_id.

    Returns:
        DataFrame: A dataframe with an additional column 'time_since_dormant'.
    """
    w = Window.partitionBy(IndCQC.location_id).orderBy(IndCQC.cqc_location_import_date)

    df = df.withColumn(
        IndCQC.dormant_date,
        F.when(
            F.col(IndCQC.dormancy) == Dormancy.dormant,
            F.col(IndCQC.cqc_location_import_date),
        ),
    )

    df = df.withColumn(
        IndCQC.last_dormant_date,
        F.last(IndCQC.dormant_date, ignorenulls=True).over(w),
    )

    df = df.withColumn(
        IndCQC.time_since_dormant,
        F.when(
            F.col(IndCQC.last_dormant_date).isNotNull(),
            F.when(F.col(IndCQC.dormancy) == Dormancy.dormant, 1).otherwise(
                F.floor(
                    F.months_between(
                        F.col(IndCQC.cqc_location_import_date),
                        F.col(IndCQC.last_dormant_date),
                    )
                )
                + 1,
            ),
        ),
    )

    df = df.drop(
        IndCQC.dormant_date,
        IndCQC.last_dormant_date,
    )

    return df


def calculate_care_home_status_count(df: DataFrame) -> DataFrame:
    """
    Calculate how many care home statuses each location has had.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with care home status count.
    """
    w = Window.partitionBy(IndCQC.location_id)

    df = df.withColumn(
        IndCQC.care_home_status_count,
        F.size((F.collect_set(IndCQC.care_home).over(w))),
    )
    return df


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
