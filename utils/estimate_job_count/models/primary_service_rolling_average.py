from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
from dataclasses import dataclass

from utils.utils import convert_days_to_unix_time
from utils.estimate_job_count.column_names import (
    UNIX_TIME,
    JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)


@dataclass
class ColName:
    count_of_job_count: str = "count_of_job_count"
    sum_of_job_count: str = "sum_of_job_count"
    rolling_total_count_of_job_count: str = "rolling_total_count_of_job_count"
    rolling_total_sum_of_job_count: str = "rolling_total_sum_of_job_count"
    rolling_average_model: str = "rolling_average_model"


def model_primary_service_rolling_average(
    df: DataFrame, number_of_days: int
) -> DataFrame:

    df_with_job_count_only = filter_to_locations_with_known_job_count(df)

    job_count_sum_and_count_df = (
        calculate_job_count_aggregates_per_service_and_time_period(
            df_with_job_count_only
        )
    )

    rolling_average_df = create_rolling_average_column(
        job_count_sum_and_count_df, number_of_days
    )

    df = join_rolling_average_into_df(df, rolling_average_df)

    return df


def filter_to_locations_with_known_job_count(df: DataFrame) -> DataFrame:
    return df.where((F.col(JOB_COUNT).isNotNull()) & (F.col(JOB_COUNT) > 0))


def calculate_job_count_aggregates_per_service_and_time_period(
    df: DataFrame,
) -> DataFrame:
    return df.groupBy(PRIMARY_SERVICE_TYPE, UNIX_TIME).agg(
        F.count(JOB_COUNT).cast("integer").alias(ColName.count_of_job_count),
        F.sum(JOB_COUNT).alias(ColName.sum_of_job_count),
    )


def create_rolling_average_column(df: DataFrame, number_of_days: int) -> DataFrame:

    df = calculate_rolling_sum(
        df,
        ColName.sum_of_job_count,
        number_of_days,
        ColName.rolling_total_sum_of_job_count,
    )
    df = calculate_rolling_sum(
        df,
        ColName.count_of_job_count,
        number_of_days,
        ColName.rolling_total_count_of_job_count,
    )

    return df.withColumn(
        ColName.rolling_average_model,
        F.col(ColName.rolling_total_sum_of_job_count)
        / F.col(ColName.rolling_total_count_of_job_count),
    )


def calculate_rolling_sum(
    df: DataFrame, col_to_sum: str, number_of_days: int, new_col_name: str
) -> DataFrame:
    return df.withColumn(
        new_col_name,
        F.sum(col_to_sum).over(define_window_specifications(UNIX_TIME, number_of_days)),
    )


def define_window_specifications(unix_date_col: str, number_of_days: int) -> Window:
    return (
        Window.partitionBy(PRIMARY_SERVICE_TYPE)
        .orderBy(F.col(unix_date_col).cast("long"))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )


def join_rolling_average_into_df(
    df: DataFrame, rolling_average_df: DataFrame
) -> DataFrame:
    rolling_average_df = rolling_average_df.select(
        PRIMARY_SERVICE_TYPE, UNIX_TIME, ColName.rolling_average_model
    )

    return df.join(rolling_average_df, [PRIMARY_SERVICE_TYPE, UNIX_TIME], "left")
