from dataclasses import fields

from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
    PrimaryServiceRollingAverageColumns as TempCol,
)
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.models.interpolation import model_interpolation
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value
from utils.utils import convert_days_to_unix_time


def model_primary_service_rolling_average_and_rate_of_change(
    df: DataFrame,
    ratio_column_to_average: str,
    posts_column_to_average: str,
    number_of_days: int,
    rolling_average_model_column_name: str,
    rolling_rate_of_change_model_column_name: str,
) -> DataFrame:
    """
    Calculates the rolling average and rate of change split by primary service type of specified columns over a given window of days.

    Calculates the rolling average and rate of change of specified columns over a given window of days partitioned by primary service type.
    Only data from locations who have at least 2 submissions and a consistent care_home status throughout time are included in the calculations.
    One day is removed from the provided number_of_days value because the pyspark range between function is inclusive at both the start and end point whereas we only want it to be inclusive of the end point.
    For example, for a 3 day rolling average we want the current day plus the two days prior.

    Args:
        df (DataFrame): The input DataFrame.
        ratio_column_to_average (str): The name of the filled posts per bed ratio column to average (for care homes only).
        posts_column_to_average (str): The name of the filled posts column to average.
        number_of_days (int): The number of days to include in the rolling average time period (where three days refers to the current day plus the previous two).
        rolling_average_model_column_name (str): The name of the new column to store the filled posts rolling average.
        rolling_rate_of_change_model_column_name (str): The name of the new column to store the rolling rate of change model values.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average and rolling rate of change modelled outputs.
    """
    number_of_days_for_window: int = number_of_days - 1

    df = create_single_column_to_average(
        df, ratio_column_to_average, posts_column_to_average
    )
    df = clean_column_to_average(df)
    df = interpolate_column_to_average(df)

    df = calculate_rolling_average(
        df, number_of_days_for_window, rolling_average_model_column_name
    )

    df = calculate_rolling_rate_of_change(
        df, number_of_days_for_window, rolling_rate_of_change_model_column_name
    )

    columns_to_drop = [field.name for field in fields(TempCol())]
    df = df.drop(*columns_to_drop)

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


def calculate_rolling_average(
    df: DataFrame, number_of_days: int, rolling_average_model_column_name: str
) -> DataFrame:
    """
    Calculates the filled post rolling average of a specified column over a given window of days partitioned by primary service type.

    Calculates the filled post rolling average of a specified column over a given window of days partitioned by primary service type.
    Non-care homes figures are already represented as filled posts, whereas for care homes the column contains a ratio which needs to be multiplied by the number of beds to get the equivalent filled posts.

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling average time period (where three days refers to the current day plus the previous two).
        rolling_average_model_column_name (str): The name of the new column to store the filled posts rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the calculated rolling average.
    """
    window = (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    rolling_average = F.avg(TempCol.column_to_average_interpolated).over(window)

    df = df.withColumn(
        rolling_average_model_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            rolling_average * F.col(IndCqc.number_of_beds),
        ).otherwise(rolling_average),
    )
    return df


def calculate_rolling_rate_of_change(
    df: DataFrame, number_of_days: int, rate_of_change_model_column_name: str
) -> DataFrame:
    """
    Calculates the rolling rate of change of a specified column over a given window of days partitioned by primary service type.

    This function sequentially calls other functions to:
    1. Add a column with previous values.
    2. Adds the two rolling sums over a specified number of days.
    3. Calculate the rate of change for a single period.
    4. Calculate the rolling rate of change model.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        number_of_days (int): The number of days to include in the rolling time period.
        rate_of_change_model_column_name (str): The name of the column to store the rate of change model.

    Returns:
        DataFrame: The DataFrame with the calculated rolling rate of change.
    """
    df = add_previous_value_column(df)
    df = add_rolling_sum(
        df,
        number_of_days,
        TempCol.column_to_average_interpolated,
        TempCol.rolling_current_period_sum,
    )
    df = add_rolling_sum(
        df,
        number_of_days,
        TempCol.previous_column_to_average_interpolated,
        TempCol.rolling_previous_period_sum,
    )
    df = calculate_single_period_rate_of_change(df)
    deduped_df = deduplicate_dataframe(df)
    cumulative_rate_of_change_df = calculate_cumulative_rate_of_change(
        deduped_df, rate_of_change_model_column_name
    )
    df = join_dataframes(df, cumulative_rate_of_change_df)

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
        TempCol.column_to_average_interpolated,
        TempCol.column_to_average_interpolated,
        TempCol.previous_column_to_average_interpolated,
        "last",
    )
    return df


def add_rolling_sum(
    df: DataFrame, number_of_days: int, column_to_sum: str, rolling_sum_column_name: str
) -> DataFrame:
    """
    Adds a rolling sum column to a DataFrame based on a specified number of days.

    Adds a rolling sum column to a DataFrame based on a specified number of days.
    Only values where both the current and previously interpolated values are known are included in the sum.

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling time period.
        column_to_sum (str): The name of the column to sum.
        rolling_sum_column_name (str): The name of the new column to store the rolling sum.

    Returns:
        DataFrame: The DataFrame with the new rolling sum column added.

    """
    both_periods_not_null = (
        F.col(TempCol.column_to_average_interpolated).isNotNull()
        & F.col(TempCol.previous_column_to_average_interpolated).isNotNull()
    )

    rolling_sum_window = (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    df = df.withColumn(
        rolling_sum_column_name,
        F.sum(F.when(both_periods_not_null, F.col(column_to_sum))).over(
            rolling_sum_window
        ),
    )
    return df


# TODO - untested
def calculate_single_period_rate_of_change(df: DataFrame) -> DataFrame:
    """
    Calculates the rate of change from the 'previous' to the 'current' (at that point in time) period.

    The rate of change is calculated as the ratio of the rolling current period sum to the rolling previous period sum.
    The rate of change is always null for the first period, so it is replaced with 1 (equivalent to 'no change').

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with the single period rate of change column added.

    """
    df = df.withColumn(
        TempCol.single_period_rate_of_change,
        F.col(TempCol.rolling_current_period_sum)
        / F.col(TempCol.rolling_previous_period_sum),
    )
    df = df.na.fill({TempCol.single_period_rate_of_change: 1.0})
    return df


# TODO - untested
def deduplicate_dataframe(df: DataFrame) -> DataFrame:
    """
    Deduplicates the DataFrame based on primary service type and unix time.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The deduplicated DataFrame.
    """
    df = df.select(
        IndCqc.primary_service_type,
        IndCqc.unix_time,
        TempCol.single_period_rate_of_change,
    ).dropDuplicates([IndCqc.primary_service_type, IndCqc.unix_time])
    return df


# TODO - untested
def calculate_cumulative_rate_of_change(
    df: DataFrame, rate_of_change_model_column_name: str
) -> DataFrame:
    """
    Calculates the cumulative rate of change for a DataFrame.

    The cumulative rate of change is a multiplication of all the single rates of change up to and including that point.
    For example, period one is 'a', period two is 'a*b', period three is 'a*b*c', etc.
    The cumulative product is calculated by taking the exponential of the sum of the logarithms of the values.
    This approach avoids issues in pyspark with direct multiplication of many numbers.

    Args:
        df (DataFrame): The input DataFrame.
        rate_of_change_model_column_name (str): The name of the new column to store the cumulative rate of change.

    Returns:
        DataFrame: The DataFrame with the cumulative rate of change column added.
    """
    w = Window.partitionBy(IndCqc.primary_service_type).orderBy(IndCqc.unix_time)

    cumulative_rate_of_change = F.exp(
        F.sum(F.log(TempCol.single_period_rate_of_change)).over(w)
    )

    df = df.withColumn(
        rate_of_change_model_column_name, cumulative_rate_of_change
    ).drop(TempCol.single_period_rate_of_change)
    return df


# TODO - untested
def join_dataframes(
    df: DataFrame, cumulative_rate_of_change_df: DataFrame
) -> DataFrame:
    """
    Joins the original DataFrame with the cumulative rate of change DataFrame.

    Args:
        df (DataFrame): The original DataFrame.
        cumulative_rate_of_change_df (DataFrame): The DataFrame with the cumulative rate of change.

    Returns:
        DataFrame: The joined DataFrame.
    """
    return df.join(
        cumulative_rate_of_change_df,
        [IndCqc.primary_service_type, IndCqc.unix_time],
        "left",
    )
