from pyspark.sql import DataFrame, Window, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.utils import convert_days_to_unix_time


def calculate_prediction_rolling_average(
    df: DataFrame, number_of_days: int
) -> DataFrame:
    """
    Calculate the rolling average of the "prediction" column over a specified window of days.

    This function calculates the rolling average for each "location_ID" based on a given number of days.
    Since the PySpark `rangeBetween` function is inclusive on both ends, one day is subtracted from
    the provided `window_days` value to ensure the window includes only the current day and the specified
    number of prior days. For example, a 3-day rolling average includes the current day plus the two preceding days.

    Args:
        df (DataFrame): A DataFrame containing "location_id", "unix_time", and "prediction" columns.
        number_of_days (int): The number of days for the rolling window.

    Returns:
        DataFrame: A new DataFrame with an additional column "prediction_rolling_average".
    """
    number_of_days_for_window: int = number_of_days - 1

    window_spec = (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days_for_window), 0)
    )

    df = df.withColumn(
        IndCqc.prediction_rolling_average, F.avg(IndCqc.prediction).over(window_spec)
    )

    return df
