from pyspark.sql import DataFrame, functions as F, Window

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.column_values.categorical_column_values import CareHome


def model_primary_service_rolling_average(
    df: DataFrame,
    filled_posts_per_bed_ratio_column_to_average: str,
    filled_post_column_to_average: str,
    number_of_days: int,
    model_filled_posts_per_bed_ratio_column_name: str,
    model_filled_posts_column_name: str,
) -> DataFrame:
    """
    Calculates the rolling average of specified columns over a given window of days.

    Calculates the rolling average of specified columns over a given window of days for care homes and non residential locations separately. The
    additional columns will be added with the column names 'model_filled_posts_column_name' and 'model_filled_posts_per_bed_ratio_column_name'.

    Args:
        df (DataFrame): The input DataFrame.
        filled_posts_per_bed_ratio_column_to_average (str): The name of the column to average for care homes.
        filled_post_column_to_average (str): The name of the column to average for non residential locations.
        number_of_days (int): The number of days to include in the rolling average time period.
        model_filled_posts_per_bed_ratio_column_name (str): The name of the column to store the care home ratio rolling average.
        model_filled_posts_column_name (str): The name of the new column to store the rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average.
    """
    window = define_window_specifications(number_of_days)
    temp_col = "temp_col"
    df = run_window_calculations(
        df,
        filled_posts_per_bed_ratio_column_to_average,
        filled_post_column_to_average,
        window,
        temp_col,
    )
    df = allocate_averages_to_columns(
        df,
        model_filled_posts_column_name,
        model_filled_posts_per_bed_ratio_column_name,
        temp_col,
    )

    return df


def allocate_averages_to_columns(
    df: DataFrame,
    model_filled_posts_column_name: str,
    model_filled_posts_per_bed_ratio_column_name: str,
    temp_col: str,
) -> DataFrame:
    """
    Allocates values from a temporary column to the correctly labelled columns.

    A temporary column is used so that the window functions can be run simultaneously to improve spark performance.

    Args:
        df (DataFrame): The input DataFrame.
        model_filled_posts_column_name (str): The name of the new column to store the rolling average.
        model_filled_posts_per_bed_ratio_column_name (str): The name of the column to store the care home ratio.
        temp_col (str): A temporary column containing the values before they are correctly allocated.

    Returns:
        DataFrame: The input DataFrame with the correctly labelled columns and without the temporary column.
    """
    df = df.withColumn(
        model_filled_posts_per_bed_ratio_column_name,
        F.when(F.col(IndCqc.care_home) == CareHome.care_home, (F.col(temp_col))),
    )
    df = df.withColumn(
        model_filled_posts_column_name,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            (F.col(temp_col)) * F.col(IndCqc.number_of_beds),
        ).otherwise(F.col(temp_col)),
    )
    df = df.drop(temp_col)

    return df


def run_window_calculations(
    df: DataFrame,
    filled_posts_per_bed_ratio_column_to_average: str,
    filled_post_column_to_average: str,
    window: Window,
    temp_col: str,
) -> DataFrame:
    """
    Calculates the rolling average of a specified column over a given window of days for care home and non residential separately.

    A temporary column is used so that the window functions can be run simultaneously to improve spark performance.

    Args:
        df (DataFrame): The input DataFrame.
        filled_posts_per_bed_ratio_column_to_average (str): The name of the column to average for care homes.
        filled_post_column_to_average (str): The name of the column to average for non residential locations.
        window (Window): The window spec to use.
        temp_col (str): A temporary column to store the values before they are correctly allocated.

    Returns:
        DataFrame: The input DataFrame with the new temporary column.
    """
    df = df.withColumn(
        temp_col,
        F.when(
            F.col(IndCqc.care_home) == CareHome.care_home,
            (F.avg(filled_posts_per_bed_ratio_column_to_average).over(window)),
        ).otherwise(F.avg(filled_post_column_to_average).over(window)),
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
