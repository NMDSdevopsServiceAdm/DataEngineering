from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from utils.utils import convert_days_to_unix_time
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_primary_service_rolling_average(
    df: DataFrame, number_of_days: int
) -> DataFrame:
    """
    df_with_filled_posts_only = filter_to_locations_with_known_filled_posts(df)

    filled_posts_sum_and_count_df = (
        calculate_filled_posts_aggregates_per_service_and_time_period(
            df_with_filled_posts_only
        )
    )

    df_with_aggregates = join_aggregates_into_df(df, filled_posts_sum_and_count_df)
    """
    df = add_column_to_flag_if_included_in_count(df)
    rolling_average_df = create_rolling_average_column(df, number_of_days)
    rolling_average_df.select(
        IndCqc.location_id,
        IndCqc.ascwds_filled_posts_dedup_clean,
        "include_in_count",
        IndCqc.rolling_total_count_of_filled_posts,
        IndCqc.rolling_total_sum_of_filled_posts,
        IndCqc.rolling_average_model,
    ).sort(IndCqc.location_id).show()
    # df = join_rolling_average_into_df(df, rolling_average_df)

    return rolling_average_df


def filter_to_locations_with_known_filled_posts(df: DataFrame) -> DataFrame:
    df = df.where(
        (F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull())
        & (F.col(IndCqc.ascwds_filled_posts_dedup_clean) > 0)
    )
    return df


def calculate_filled_posts_aggregates_per_service_and_time_period(
    df: DataFrame,
) -> DataFrame:
    df = df.groupBy(IndCqc.primary_service_type, IndCqc.unix_time).agg(
        F.count(IndCqc.ascwds_filled_posts_dedup_clean)
        .cast("integer")
        .alias(IndCqc.count_of_filled_posts),
        F.sum(IndCqc.ascwds_filled_posts_dedup_clean).alias(IndCqc.sum_of_filled_posts),
    )
    return df


def add_column_to_flag_if_included_in_count(df: DataFrame):
    df = df.withColumn(
        "include_in_count",
        F.when(df[IndCqc.ascwds_filled_posts_dedup_clean].isNotNull(), 1).otherwise(0),
    )
    return df


def create_rolling_average_column(df: DataFrame, number_of_days: int) -> DataFrame:
    df = calculate_rolling_sum(
        df,
        IndCqc.ascwds_filled_posts_dedup_clean,
        number_of_days,
        IndCqc.rolling_total_sum_of_filled_posts,
    )
    df = calculate_rolling_sum(
        df,
        "include_in_count",
        number_of_days,
        IndCqc.rolling_total_count_of_filled_posts,
    )

    df = df.withColumn(
        IndCqc.rolling_average_model,
        F.col(IndCqc.rolling_total_sum_of_filled_posts)
        / F.col(IndCqc.rolling_total_count_of_filled_posts),
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


def join_rolling_average_into_df(
    df: DataFrame, rolling_average_df: DataFrame
) -> DataFrame:
    """
    rolling_average_df = rolling_average_df.select(
        IndCqc.primary_service_type, IndCqc.unix_time, IndCqc.rolling_average_model
    )
    """
    df = df.join(
        rolling_average_df, [IndCqc.primary_service_type, IndCqc.unix_time], "left"
    )
    return df


def join_aggregates_into_df(df: DataFrame, aggregates_df: DataFrame) -> DataFrame:
    aggregates_df = aggregates_df.select(
        IndCqc.primary_service_type,
        IndCqc.unix_time,
        IndCqc.count_of_filled_posts,
        IndCqc.sum_of_filled_posts,
    )

    df = df.join(aggregates_df, [IndCqc.primary_service_type, IndCqc.unix_time], "left")
    return df
