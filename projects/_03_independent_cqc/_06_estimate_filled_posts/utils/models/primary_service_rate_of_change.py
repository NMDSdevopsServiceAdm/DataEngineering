from dataclasses import fields
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.interpolation import (
    model_interpolation,
)
from projects.utils.utils.utils import calculate_windowed_column
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_names.ind_cqc_pipeline_columns import (
    PrimaryServiceRateOfChangeColumns as TempCol,
)
from utils.utils import convert_days_to_unix_time


def model_primary_service_rate_of_change(
    df: DataFrame,
    column_with_values: str,
    number_of_days: int,
    rate_of_change_column_name: str,
    max_days_between_submissions: Optional[int] = None,
) -> DataFrame:
    """
    Computes the rate of change since the previous period for a specified column over a rolling window, partitioned by primary service type.

    Only data from locations with at least two submissions and a consistent care home status over time are considered.

    A rolling window is applied to smooth fluctuations in the data by combining values over a specified number of days.
    This helps produce more stable and reliable trends by reducing the impact of short-term variations.

    Since the PySpark `rangeBetween` function is inclusive on both ends, one day is subtracted from the provided
    `window_days` value to ensure the window includes only the current day and the specified number of prior days.
    For example, a 3-day rolling average includes the current day plus the two preceding days.

    Args:
        df (DataFrame): Input DataFrame.
        column_with_values (str): Column name containing the values.
        number_of_days (int): Rolling window size in days (e.g., 3 includes the current day and the previous two).
        rate_of_change_column_name (str): Name of the column to store the rate of change values.
        max_days_between_submissions (Optional[int]): Maximum allowed days between submissions to apply interpolation.
                                                      If None, interpolation is applied to all rows.

    Returns:
        DataFrame: The input DataFrame with an additional column containing the rate of change values.
    """
    number_of_days_for_window: int = number_of_days - 1

    df = df.select(
        IndCqc.location_id,
        IndCqc.unix_time,
        IndCqc.care_home,
        IndCqc.care_home_status_count,
        IndCqc.primary_service_type,
        IndCqc.number_of_beds_banded_roc,
        column_with_values,
    ).withColumnRenamed(column_with_values, TempCol.current_period)

    w_spec = Window.partitionBy(IndCqc.location_id, IndCqc.care_home)
    df = calculate_windowed_column(
        df, w_spec, TempCol.submission_count, TempCol.current_period, "count"
    )

    df = remove_ineligible_locations(df)
    df = interpolate_current_values(df, max_days_between_submissions)
    df = add_previous_value_column(df)
    df = calculate_primary_service_rolling_sums(df, number_of_days_for_window)
    df = calculate_rate_of_change(df, rate_of_change_column_name)

    # deduped_df = deduplicate_dataframe(df) - TODO copy across

    # required?
    # columns_to_drop = [field.name for field in fields(TempCol())]
    # df = df.drop(*columns_to_drop)

    return df


def remove_ineligible_locations(df: DataFrame) -> DataFrame:
    """
    Only keep rows for locations who meet eligibility rules:
        - at least two submissions
        - consistent care home status over time

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with ineligible locations removed.
    """
    one_care_home_status: int = 1
    two_submissions: int = 2

    df = df.filter(
        (F.col(IndCqc.care_home_status_count) == one_care_home_status)
        & (F.col(TempCol.submission_count) >= two_submissions)
    )
    return df


def interpolate_current_values(
    df: DataFrame, max_days_between_submissions: Optional[int] = None
) -> DataFrame:
    """
    Interpolate column_with_values and coalesce with original values.

    Args:
        df (DataFrame): The input DataFrame.
        max_days_between_submissions (Optional[int]): Maximum allowed days between submissions to apply interpolation. If None, interpolation is applied to all rows.

    Returns:
        DataFrame: The input DataFrame with interpolated values.
    """
    df = model_interpolation(
        df,
        TempCol.current_period,
        "straight",
        TempCol.current_period_interpolated,
        max_days_between_submissions=max_days_between_submissions,
    )
    df = df.withColumn(
        TempCol.current_period_interpolated,
        F.coalesce(TempCol.current_period, TempCol.current_period_interpolated),
    )
    return df


def add_previous_value_column(df: DataFrame) -> DataFrame:
    """
    Adds the previous interpolated value for that location into a new column.

    Args:
        df (DataFrame): The input DataFrame containing the data.

    Returns:
        DataFrame: The DataFrame with the previously interpolated value added.
    """
    w = Window.partitionBy(IndCqc.location_id).orderBy(IndCqc.unix_time)

    df = df.withColumn(
        TempCol.previous_period_interpolated,
        F.lag(F.col(TempCol.current_period_interpolated)).over(w),
    )
    return df


def calculate_primary_service_rolling_sums(
    df: DataFrame, number_of_days: int
) -> DataFrame:
    """
    Calculates the rolling sum of the current and previous period values partitioned by primary service type.

    This function:
        1. Defines a window partitioned by primary service type and banded number of beds, ordered by unix time, with a range between the current row and the specified number of prior days.
        2. Filters the DataFrame to include only rows where both current and previous period interpolated values are known (non-null).
        3. Calculates the rolling sum of the current and previous period interpolated values over the defined window.
        4. Drops duplicate rows based on the partitioning columns and unix time to ensure one row per primary service type, number of beds band, and time period.
        5. Drops non-aggregated columns that are no longer needed for subsequent calculations.

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling time period.

    Returns:
        DataFrame: The DataFrame with the two new rolling sum columns added.
    """
    current_col = F.col(TempCol.current_period_interpolated)
    previous_col = F.col(TempCol.previous_period_interpolated)
    partition_cols = [IndCqc.primary_service_type, IndCqc.number_of_beds_banded_roc]

    window = (
        Window.partitionBy(partition_cols)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    rolling_sum_df = (
        df.filter(current_col.isNotNull() & previous_col.isNotNull())
        .withColumns(
            {
                TempCol.rolling_current_sum: F.sum(current_col).over(window),
                TempCol.rolling_previous_sum: F.sum(previous_col).over(window),
            },
        )
        .dropDuplicates(partition_cols + [IndCqc.unix_time])
        .drop(IndCqc.location_id, current_col, previous_col)
    )

    return rolling_sum_df


def calculate_rate_of_change(
    df: DataFrame, rate_of_change_column_name: str
) -> DataFrame:
    """
    Calculates the rate of change from the 'previous' to the 'current' (at that point in time) period.

    The rate of change is calculated as the ratio of the rolling current period sum to the rolling previous period sum.
    The rate of change is always null for the first period, so it is replaced with 1 (equivalent to 'no change').

    Args:
        df (DataFrame): The input DataFrame.
        rate_of_change_column_name (str): Name of the column to store the rate of change values.

    Returns:
        DataFrame: The DataFrame with the single period rate of change column added.
    """
    df = df.withColumn(
        rate_of_change_column_name,
        F.col(TempCol.rolling_current_sum) / F.col(TempCol.rolling_previous_sum),
    )
    df = df.na.fill({rate_of_change_column_name: 1.0})
    return df
