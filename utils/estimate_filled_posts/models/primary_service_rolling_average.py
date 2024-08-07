from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_primary_service_rolling_average(
    df: DataFrame, column_to_average: str, number_of_days: int
) -> DataFrame:
    df = add_flag_if_included_in_count(df, column_to_average)
    rolling_average_df = create_rolling_average_column(
        df, column_to_average, number_of_days
    )
    return rolling_average_df


def add_flag_if_included_in_count(df: DataFrame, column_to_average: str):
    df = df.withColumn(
        IndCqc.include_in_rolling_average_count,
        F.when(F.col(column_to_average).isNotNull(), F.lit(1)).otherwise(F.lit(0)),
    )
    return df


def create_rolling_average_column(
    df: DataFrame, column_to_average: str, number_of_days: int
) -> DataFrame:
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
        IndCqc.rolling_average_model,
        F.col(IndCqc.rolling_sum) / F.col(IndCqc.rolling_count),
    )
    df = df.drop(
        IndCqc.include_in_rolling_average_count,
        IndCqc.rolling_count,
        IndCqc.rolling_sum,
    )
    return df


def calculate_rolling_sum(
    df: DataFrame, col_to_sum: str, number_of_days: int, new_col_name: str
) -> DataFrame:
    df = df.withColumn(
        new_col_name,
        F.sum(col_to_sum).over(
            define_window_specifications(IndCqc.unix_time, number_of_days)
        ),
    )
    return df


def define_window_specifications(unix_date_col: str, number_of_days: int) -> Window:
    return (
        Window.partitionBy(IndCqc.primary_service_type)
        .orderBy(F.col(unix_date_col).cast("long"))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )
