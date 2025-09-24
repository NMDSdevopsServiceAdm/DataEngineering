from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

import utils.cleaning_utils as cUtils
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.primary_service_rate_of_change import (
    model_primary_service_rate_of_change,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_primary_service_rate_of_change_trendline(
    df: DataFrame,
    column_with_values: str,
    number_of_days: int,
    rate_of_change_trendline_column_name: str,
    max_days_between_submissions: Optional[int] = None,
) -> DataFrame:
    """
    Computes a trendline from a sequence of single-period rate of change values, starting at 1.0 in the first period.

    This function first calls the 'primary_service_rate_of_change' function to compute the rate of change since the
    previous period for a specified column over a rolling window, partitioned by primary service type.

    The trendline is then derived by iteratively multiplying each rate of change value, resulting in a cumulative
    measure of change over time.

    Example:
        Given a rate of change sequence:
            - Period 2: 1.01 (1.0% increase from period 1 to 2)
            - Period 3: 1.02 (2.0% increase from period 2 to 3)
            - Period 4: 0.99 (1.0% decrease from period 3 to 4)
        The computed trendline:
            - Period 1: 1.0 (no change)
            - Period 2: 1.01 (1.0% increase from period 1 to 2)
            - Period 3: 1.01 * 1.02 = 1.0301 (3.0% increase from period 1 to 3)
            - Period 4: 1.01 * 1.02 * 0.99 = 1.02 (2.0% increase from period 1 to 4)

    Args:
        df (DataFrame): The input DataFrame.
        column_with_values (str): Column name containing the values.
        number_of_days (int): Rolling window size in days (e.g., 3 includes the current day and the previous two).
        rate_of_change_trendline_column_name (str): Name of the new column to store the cumulative trendline.
        max_days_between_submissions (Optional[int]): Maximum allowed days between submissions to apply interpolation.
                                                      If None, interpolation is applied to all rows.

    Returns:
        DataFrame: The DataFrame with the trendline column.
    """

    df = cUtils.create_banded_bed_count_column(
        df,
        IndCqc.number_of_beds_banded_for_rate_of_change,
        [0, 1, 15, 25, float("Inf")],
    )

    df = model_primary_service_rate_of_change(
        df,
        column_with_values,
        number_of_days,
        IndCqc.single_period_rate_of_change,
        max_days_between_submissions,
    )

    deduped_df = deduplicate_dataframe(df)

    rate_of_change_trendline_df = calculate_rate_of_change_trendline(
        deduped_df, rate_of_change_trendline_column_name
    )

    df = df.join(
        rate_of_change_trendline_df,
        [
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_for_rate_of_change,
            IndCqc.unix_time,
        ],
        "left",
    )

    df = df.drop(
        IndCqc.number_of_beds_banded_for_rate_of_change,
        IndCqc.single_period_rate_of_change,
    )

    return df


def deduplicate_dataframe(df: DataFrame) -> DataFrame:
    """
    Selects primary service type, banded beds, unix time and single period rate of change then deduplicates the DataFrame based on primary service type and unix time.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The deduplicated DataFrame.
    """
    df = df.select(
        IndCqc.primary_service_type,
        IndCqc.number_of_beds_banded_for_rate_of_change,
        IndCqc.unix_time,
        IndCqc.single_period_rate_of_change,
    ).dropDuplicates(
        [
            IndCqc.primary_service_type,
            IndCqc.number_of_beds_banded_for_rate_of_change,
            IndCqc.unix_time,
        ]
    )

    return df


def calculate_rate_of_change_trendline(
    df: DataFrame,
    rate_of_change_trendline_column_name: str,
) -> DataFrame:
    """
    Computes a trendline from a sequence of single-period rate of change values, starting at 1.0 in the first period.

    The trendline is then derived by iteratively multiplying each rate of change value, resulting in a cumulative
    measure of change over time.
    This is calculated by taking the exponential of the sum of the logarithms of the values.
    This approach avoids issues in pyspark with direct multiplication of many numbers.

    Args:
        df (DataFrame): The input DataFrame.
        rate_of_change_trendline_column_name (str): Name of the new column to store the rate of change trendline.

    Returns:
        DataFrame: The DataFrame with the rate of change trendline included.
    """
    w = Window.partitionBy(
        IndCqc.primary_service_type, IndCqc.number_of_beds_banded_for_rate_of_change
    ).orderBy(IndCqc.unix_time)

    trendline_df = df.withColumn(
        rate_of_change_trendline_column_name,
        F.exp(F.sum(F.log(IndCqc.single_period_rate_of_change)).over(w)),
    )

    trendline_df = trendline_df.drop(IndCqc.single_period_rate_of_change)

    return trendline_df
