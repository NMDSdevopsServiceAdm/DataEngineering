from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_primary_service_rolling_average(
    df: DataFrame, column_to_average: str, number_of_days: int, model_column_name: str
) -> DataFrame:
    """
    Calculates the rolling average of a specified column over a given window of days.

    Calculates the rolling average of a specified column over a given window of days. In order to calculate
    the average this function first creates a rolling sum and a rolling count of the column values to
    include in the calculation. The average is calculated by dividing the sum by the count. Temporary
    columns created during the calculation process are dropped before returning the final dataframe. The only
    additional column added will be the rolling average with the column name 'model_column_name'.

    Args:
        df (DataFrame): The input DataFrame.
        column_to_average (str): The name of the column to average.
        number_of_days (int): The number of days to include in the rolling average time period.
        model_column_name (str): The name of the new column to store the rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average.
    """
    df = add_flag_if_included_in_count(df, column_to_average)

    df = calculate_rolling_sum(
        df,
        column_to_average,
        number_of_days,
        IndCqc.rolling_sum,
    )
    df = calculate_rolling_sum(
        df,
        IndCqc.include_in_rolling_average_count,
        number_of_days,
        IndCqc.rolling_count,
    )

    df = df.withColumn(
        model_column_name,
        F.col(IndCqc.rolling_sum) / F.col(IndCqc.rolling_count),
    )
    df = df.drop(
        IndCqc.include_in_rolling_average_count,
        IndCqc.rolling_count,
        IndCqc.rolling_sum,
    )
    return df


def add_flag_if_included_in_count(df: DataFrame, column_to_average: str):
    df = df.withColumn(
        IndCqc.include_in_rolling_average_count,
        F.when(F.col(column_to_average).isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
    return df


def calculate_rolling_sum(
    df: DataFrame, col_to_sum: str, number_of_days: int, new_col_name: str
) -> DataFrame:
    """
    Calculates the rolling sum of a specified column over a given window of days.

    Calculates the rolling sum of a specified column over a given window of days.

    Args:
        df (DataFrame): The input DataFrame.
        col_to_sum (str): The name of the column to sum.
        number_of_days (int): The number of days to include in the rolling sum time period.
        new_col_name (str): The name of the new column to store the rolling sum.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling sum.
    """
    df = df.withColumn(
        new_col_name,
        F.sum(col_to_sum).over(
            define_window_specifications(IndCqc.unix_time, number_of_days)
        ),
    )
    return df


def define_window_specifications(unix_date_col: str, number_of_days: int) -> Window:
    """
    Define the Window specification partitioned by primary service column.

    Define the Window specification partitioned by primary service column.

    Args:
        unix_date_col (str): The name of the column containing unix timestamps.
        number_of_days (int): The number of days to use for the rolling average calculations.

    Returns:
        Window: The required Window specification partitioned by primary service column.
    """
    return (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(unix_date_col).cast("long"))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )
