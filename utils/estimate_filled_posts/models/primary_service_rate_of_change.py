from dataclasses import fields

from pyspark.sql import DataFrame, functions as F, Window
from typing import Optional

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    PrimaryServiceRateOfChangeColumns as TempCol,
)
from utils.estimate_filled_posts.models.interpolation import model_interpolation
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value
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

    df = df.withColumn(TempCol.column_with_values, F.col(column_with_values))

    df = clean_column_with_values(df)
    df = interpolate_column_with_values(df, max_days_between_submissions)
    df = add_previous_value_column(df)
    df = add_rolling_sum_columns(df, number_of_days_for_window)
    df = calculate_rate_of_change(df, rate_of_change_column_name)

    columns_to_drop = [field.name for field in fields(TempCol())]
    df = df.drop(*columns_to_drop)

    return df


def clean_column_with_values(df: DataFrame) -> DataFrame:
    """
    Keep values for locations who have submitted at least twice and have only ever had one care home status.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with unwanted data nulled.
    """
    one_care_home_status: int = 1
    two_submissions: int = 2

    df = calculate_care_home_status_count(df)
    df = calculate_submission_count(df)
    df = df.withColumn(
        TempCol.column_with_values,
        F.when(
            (F.col(TempCol.care_home_status_count) == one_care_home_status)
            & (F.col(TempCol.submission_count) >= two_submissions),
            F.col(TempCol.column_with_values),
        ).otherwise(F.lit(None)),
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
    w = Window.partitionBy(IndCqc.location_id)

    df = df.withColumn(
        TempCol.care_home_status_count,
        F.size((F.collect_set(IndCqc.care_home).over(w))),
    )
    return df


def calculate_submission_count(df: DataFrame) -> DataFrame:
    """
    Calculate how many submissions each location has made.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with submission count.
    """
    w = Window.partitionBy(IndCqc.location_id, IndCqc.care_home)

    df = df.withColumn(
        TempCol.submission_count, F.count(TempCol.column_with_values).over(w)
    )
    return df


def interpolate_column_with_values(
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
        TempCol.column_with_values,
        "straight",
        TempCol.column_with_values_interpolated,
        max_days_between_submissions=max_days_between_submissions,
    )
    df = df.withColumn(
        TempCol.column_with_values_interpolated,
        F.coalesce(TempCol.column_with_values, TempCol.column_with_values_interpolated),
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
    location_window = (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(IndCqc.unix_time)
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    df = get_selected_value(
        df,
        location_window,
        TempCol.column_with_values_interpolated,
        TempCol.column_with_values_interpolated,
        TempCol.previous_column_with_values_interpolated,
        "last",
    )
    return df


def add_rolling_sum_columns(df: DataFrame, number_of_days: int) -> DataFrame:
    """
    Adds rolling sum columns for the current and previous period to a DataFrame over a specified number of days.

    The rolling sum includes only rows where both the current and previous interpolated values are not null.

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling time period.

    Returns:
        DataFrame: The DataFrame with the two new rolling sum columns added.
    """
    valid_rows = (
        F.col(TempCol.column_with_values_interpolated).isNotNull()
        & F.col(TempCol.previous_column_with_values_interpolated).isNotNull()
    )

    rolling_sum_window = (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    df = df.withColumn(
        TempCol.rolling_current_period_sum,
        F.sum(F.when(valid_rows, F.col(TempCol.column_with_values_interpolated))).over(
            rolling_sum_window
        ),
    )
    df = df.withColumn(
        TempCol.rolling_previous_period_sum,
        F.sum(
            F.when(valid_rows, F.col(TempCol.previous_column_with_values_interpolated))
        ).over(rolling_sum_window),
    )
    return df


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
        F.col(TempCol.rolling_current_period_sum)
        / F.col(TempCol.rolling_previous_period_sum),
    )
    df = df.na.fill({rate_of_change_column_name: 1.0})
    return df
