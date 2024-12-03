from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.models.interpolation import model_interpolation
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value
from utils.utils import convert_days_to_unix_time


@dataclass
class TempCol:
    """The names of the temporary columns created during the rolling average process."""

    care_home_status_count: str = "care_home_status_count"
    column_to_average: str = "column_to_average"
    column_to_average_interpolated: str = "column_to_average_interpolated"
    submission_count: str = "submission_count"
    temp_rolling_average: str = "temp_rolling_average"


def model_primary_service_rolling_average(
    df: DataFrame,
    ratio_column_to_average: str,
    posts_column_to_average: str,
    number_of_days: int,
    ratio_rolling_average_model_column_name: str,
    posts_rolling_average_model_column_name: str,
) -> DataFrame:
    """
    Calculates the rolling average split by primary service type of specified columns over a given window of days (where three days refers to the current day plus the previous two).

    Calculates the rolling average of specified columns over a given window of days partitioned by primary service type.
    Only data from locations who have at least 2 submissions and a consistent care_home status throughout time are included in the calculations.
    The additional columns will be added with the column names 'posts_rolling_average_model_column_name' and 'ratio_rolling_average_model_column_name'.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column_to_average (str): The name of the filled posts per bed ratio column to average (for care homes only).
        posts_column_to_average (str): The name of the filled posts column to average.
        number_of_days (int): The number of days to include in the rolling average time period (where three days refers to the current day plus the previous two).
        ratio_rolling_average_model_column_name (str): The name of the new column to store the care home filled posts per bed ratio rolling average.
        posts_rolling_average_model_column_name (str): The name of the new column to store the filled posts rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the two new rolling average columns.
    """
    df = create_single_column_to_average(
        df, ratio_column_to_average, posts_column_to_average
    )
    df = clean_column_to_average(df)
    df = interpolate_column_to_average(df)

    # New approach A
    df = calculate_rolling_average(df, number_of_days)
    df = create_final_model_columns(
        df,
        ratio_rolling_average_model_column_name,
        posts_rolling_average_model_column_name,
    )

    # New approach B
    window_spec_lagged = (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(IndCqc.unix_time)
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    df = get_selected_value(
        df,
        window_spec_lagged,
        TempCol.column_to_average_interpolated,
        TempCol.column_to_average_interpolated,
        "prev_column_to_average_interpolated",
        "last",
    )
    number_of_days_for_window: int = number_of_days - 1

    one_window = Window.partitionBy(IndCqc.primary_service_type, IndCqc.unix_time)
    both_periods_not_null = (
        F.col(TempCol.column_to_average_interpolated).isNotNull()
        & F.col("prev_column_to_average_interpolated").isNotNull()
    )

    df = df.withColumn(
        "current_period_sum",
        F.sum(
            F.when(both_periods_not_null, F.col(TempCol.column_to_average_interpolated))
        ).over(one_window),
    )
    df = df.withColumn(
        "previous_period_sum",
        F.sum(
            F.when(both_periods_not_null, F.col("prev_column_to_average_interpolated"))
        ).over(one_window),
    )

    df = df.withColumn(
        "rate_of_change_one_period",
        F.sum(F.col("current_period_sum")) / F.sum(F.col("previous_period_sum")),
    )

    rolling_roc_window = (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days_for_window), 0)
    )

    df = df.withColumn(
        "rolling_current_period_sum",
        F.sum(
            F.when(both_periods_not_null, F.col(TempCol.column_to_average_interpolated))
        ).over(rolling_roc_window),
    )
    df = df.withColumn(
        "rolling_previous_period_sum",
        F.sum(
            F.when(both_periods_not_null, F.col("prev_column_to_average_interpolated"))
        ).over(rolling_roc_window),
    )

    df = df.withColumn(
        "rolling_rate_of_change",
        F.sum(F.col("rolling_current_period_sum"))
        / F.sum(F.col("rolling_previous_period_sum")),
    )

    df = df.drop(
        TempCol.column_to_average,
        # TempCol.column_to_average_interpolated,
        TempCol.temp_rolling_average,
    )

    return df


def create_single_column_to_average(
    df: DataFrame,
    ratio_column_to_average: str,
    posts_column_to_average: str,
) -> DataFrame:
    """
    Creates one column to average using the ratio if the location is a care home and filled posts if not.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column_to_average (str): The name of the filled posts per bed ratio column to average (for care homes only).
        posts_column_to_average (str): The name of the filled posts column to average.

    Returns:
        DataFrame: The input DataFrame with the new column containing a single column with the relevant column to average.
    """
    df = df.withColumn(
        TempCol.column_to_average,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(ratio_column_to_average),
        ).otherwise(F.col(posts_column_to_average)),
    )
    return df


def clean_column_to_average(df: DataFrame) -> DataFrame:
    """
    Only keep values in the column_to_average for locations who have only submitted at least twice and only had one care home status.

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
        TempCol.column_to_average,
        F.when(
            (F.col(TempCol.care_home_status_count) == one_care_home_status)
            & (F.col(TempCol.submission_count) >= two_submissions),
            F.col(TempCol.column_to_average),
        ).otherwise(F.lit(None)),
    )
    df = df.drop(TempCol.care_home_status_count, TempCol.submission_count)
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
        TempCol.submission_count, F.count(TempCol.column_to_average).over(w)
    )
    return df


def interpolate_column_to_average(df: DataFrame) -> DataFrame:
    """
    Interpolate column_to_average and coalesce known column_to_average values with interpolated values.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The input DataFrame with submission count.
    """
    df = model_interpolation(
        df,
        TempCol.column_to_average,
        "straight",
        TempCol.column_to_average_interpolated,
    )
    df = df.withColumn(
        TempCol.column_to_average_interpolated,
        F.coalesce(TempCol.column_to_average, TempCol.column_to_average_interpolated),
    )
    return df


def calculate_rolling_average(df: DataFrame, number_of_days: int) -> DataFrame:
    """
    Calculates the rolling average of a specified column over a given window of days partitioned by primary service type.

    Calculates the rolling average of a specified column over a given window of days partitioned by primary service type.
    One day is removed from the provided number_of_days value because the pyspark range between function is inclusive at both the start and end point whereas we only want it to be inclusive of the end point.
    For example, for a 3 day rolling average we want the current day plus the two days prior (not the three days prior).

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling average time period (where three days refers to the current day plus the previous two).

    Returns:
        DataFrame: The input DataFrame with the new column containing the calculated rolling average.
    """
    number_of_days_for_window: int = number_of_days - 1

    window = (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days_for_window), 0)
    )

    df = df.withColumn(
        TempCol.temp_rolling_average,
        F.avg(TempCol.column_to_average_interpolated).over(window),
    )
    return df


def create_final_model_columns(
    df: DataFrame,
    ratio_rolling_average_model_column_name: str,
    posts_rolling_average_model_column_name: str,
) -> DataFrame:
    """
    Allocates values from temp_rolling_average to the correctly labelled columns.

    `ratio_rolling_average_model_column_name` is only populated for care homes and replicates what is in the `temp_rolling_average`.
    `posts_rolling_average_model_column_name` replicates what is in the `temp_rolling_average` for non-care homes and for care homes, the column is multiplied by number of beds to get the equivalent filled posts.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_rolling_average_model_column_name (str): The name of the new column to store the care home filled posts per bed ratio rolling average.
        posts_rolling_average_model_column_name (str): The name of the new column to store the filled posts rolling average.

    Returns:
        DataFrame: The input DataFrame with the two model columns added.
    """
    df = df.withColumn(
        ratio_rolling_average_model_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(TempCol.temp_rolling_average),
        ),
    )
    df = df.withColumn(
        posts_rolling_average_model_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            F.col(TempCol.temp_rolling_average) * F.col(IndCqc.number_of_beds),
        ).otherwise(F.col(TempCol.temp_rolling_average)),
    )

    return df
