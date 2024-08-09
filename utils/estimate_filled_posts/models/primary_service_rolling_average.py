from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_primary_service_rolling_average(
    df: DataFrame,
    column_to_average: str,
    number_of_days: int,
    model_column_name: str,
    care_home: bool,
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
        care_home (bool): True if care home rolling average required, false if non-residential rolling average required.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average.
    """
    df = calculate_rolling_sum(
        df,
        column_to_average,
        number_of_days,
    )
    df = calculate_rolling_count(
        df,
        column_to_average,
        number_of_days,
    )
    df = calculate_rolling_average(df, model_column_name)

    df = df.drop(
        IndCqc.rolling_count,
        IndCqc.rolling_sum,
    )

    return df


def calculate_rolling_sum(
    df: DataFrame, col_to_sum: str, number_of_days: int
) -> DataFrame:
    """
    Calculates the rolling sum of a specified column over a given window of days.

    Adds a new column called rolling_sum which is the sum of non-null values in a specified
    column over a given window of days.

    Args:
        df (DataFrame): The input DataFrame.
        col_to_sum (str): The name of the column to sum non-null values.
        number_of_days (int): The number of days to include in the rolling sum time period.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling_sum.
    """
    df = df.withColumn(
        IndCqc.rolling_sum,
        F.sum(col_to_sum).over(define_window_specifications(number_of_days)),
    )
    return df


def calculate_rolling_count(
    df: DataFrame, col_to_count: str, number_of_days: int
) -> DataFrame:
    """
    Calculates the rolling count of a specified column over a given window of days.

    Adds a new column called rolling_count which is the count of non-null values in a specified
    column over a given window of days.

    Args:
        df (DataFrame): The input DataFrame.
        col_to_count (str): The name of the column to count non-null values.
        number_of_days (int): The number of days to include in the rolling count time period.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling_count.
    """
    df = df.withColumn(
        IndCqc.rolling_count,
        F.count(col_to_count).over(define_window_specifications(number_of_days)),
    )
    return df


def define_window_specifications(number_of_days: int) -> Window:
    """
    Define the Window specification partitioned by primary service column.

    Args:
        number_of_days (int): The number of days to use for the rolling average calculations.

    Returns:
        Window: The required Window specification partitioned by primary service column.
    """
    return (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(IndCqc.unix_time).cast("long"))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )


def calculate_rolling_average(df: DataFrame, model_column_name: str) -> DataFrame:
    """
    Calculates the rolling average by dividing rolling sum by rolling count.

    Adds a new column with the name provided in model_column_name which is calculated by dividing
    the rolling sum by the rolling count.

    Args:
        df (DataFrame): The input DataFrame.
        col_to_count (str): The name of the column to count.
        number_of_days (int): The number of days to include in the rolling count time period.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling_count.
    """
    df = df.withColumn(
        model_column_name,
        F.col(IndCqc.rolling_sum) / F.col(IndCqc.rolling_count),
    )
    return df
