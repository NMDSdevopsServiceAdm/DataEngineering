from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.column_values.categorical_column_values import CareHome


def model_primary_service_rolling_average(
    df: DataFrame,
    care_home_column_to_average: str,
    non_res_column_to_average: str,
    number_of_days: int,
    model_column_name: str,
) -> DataFrame:
    """
    Calculates the rolling average of a specified column over a given window of days.

    Calculates the rolling average of a specified column over a given window of days for care homes and non residential locations separately. The
    additional column will be added with the column name 'model_column_name'.

    Args:
        df (DataFrame): The input DataFrame.
        care_home_column_to_average (str): The name of the column to average for care homes.
        non_res_column_to_average (str): The name of the column to average for non residential locations.
        number_of_days (int): The number of days to include in the rolling average time period.
        model_column_name (str): The name of the new column to store the rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average.
    """
    window = define_window_specifications(number_of_days)
    df = df.withColumn(
        model_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            (F.avg(care_home_column_to_average).over(window))
            * F.col(IndCqc.number_of_beds),
        ).otherwise(F.avg(non_res_column_to_average).over(window)),
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
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )
