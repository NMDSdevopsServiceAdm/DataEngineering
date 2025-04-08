from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.utils import convert_days_to_unix_time


def model_calculate_rolling_average(
    df: DataFrame,
    column_to_average: str,
    number_of_days: int,
    column_to_partition_by: str,
    new_column_name: str,
) -> DataFrame:
    """
    Calculate the rolling average of the "column_to_average" column over a specified window of days and partition.

    This function calculates the rolling average of a column based on a given number of days and a column to partition by.
    Since the PySpark `rangeBetween` function is inclusive on both ends, one day is subtracted from
    the provided `window_days` value to ensure the window includes only the current day and the specified
    number of prior days. For example, a 3-day rolling average includes the current day plus the two preceding days.

    Args:
        df (DataFrame): The input DataFrame.
        column_to_average (str): The name of the column with the values to average.
        number_of_days (int): The number of days for the rolling window.
        column_to_partition_by (str): The name of the column to partition the window by.
        new_column_name (str): The name of the new column to store the rolling average values.

    Returns:
        DataFrame: A new DataFrame with an additional column containing the rolling average.
    """
    number_of_days_for_window: int = number_of_days - 1

    window_spec = (
        Window.partitionBy(column_to_partition_by)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days_for_window), 0)
    )

    df = df.withColumn(new_column_name, F.avg(column_to_average).over(window_spec))

    return df
