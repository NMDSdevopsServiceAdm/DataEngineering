import sys
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
import pyspark.sql

from utils.utils import convert_days_to_unix_time

from pyspark.sql.types import ArrayType, LongType, FloatType
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_interpolation(df: DataFrame) -> DataFrame:
    known_job_count_df = filter_to_locations_with_a_known_job_count(df)

    first_and_last_submission_date_df = (
        calculate_first_and_last_submission_date_per_location(known_job_count_df)
    )

    all_dates_df = convert_first_and_last_known_years_into_exploded_df(
        first_and_last_submission_date_df
    )

    all_dates_df = merge_known_values_with_exploded_dates(
        all_dates_df, known_job_count_df
    )

    all_dates_df = interpolate_values_for_all_dates(all_dates_df)

    df = leftouter_join_on_locationid_and_unix_time(df, all_dates_df)

    df = df.withColumn(
        IndCqc.estimate_filled_posts,
        F.when(
            F.col(IndCqc.estimate_filled_posts).isNotNull(),
            F.col(IndCqc.estimate_filled_posts),
        ).otherwise(F.col(IndCqc.interpolation_model)),
    )
    df = update_dataframe_with_identifying_rule(
        df, IndCqc.interpolation_model, IndCqc.estimate_filled_posts
    )

    return df


def filter_to_locations_with_a_known_job_count(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.select(
        IndCqc.location_id, IndCqc.unix_time, IndCqc.ascwds_filled_posts_dedup_clean
    )

    return df.where(F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull())


def calculate_first_and_last_submission_date_per_location(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    return df.groupBy(IndCqc.location_id).agg(
        F.min(IndCqc.unix_time).cast("integer").alias(IndCqc.first_submission_time),
        F.max(IndCqc.unix_time).cast("integer").alias(IndCqc.last_submission_time),
    )


def convert_first_and_last_known_years_into_exploded_df(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    date_range_udf = F.udf(create_date_range, ArrayType(LongType()))

    return df.withColumn(
        IndCqc.unix_time,
        F.explode(
            date_range_udf(IndCqc.first_submission_time, IndCqc.last_submission_time)
        ),
    ).drop(IndCqc.first_submission_time, IndCqc.last_submission_time)


def create_date_range(
    unix_start_time: int, unix_finish_time: int, step_size_in_days: int = 1
) -> int:
    """Return a list of equally spaced points between unix_start_time and unix_finish_time with set stepsizes"""
    unix_time_step = convert_days_to_unix_time(step_size_in_days)

    return [
        unix_start_time + unix_time_step * x
        for x in range(int((unix_finish_time - unix_start_time) / unix_time_step) + 1)
    ]


def merge_known_values_with_exploded_dates(
    df: DataFrame, known_job_count_df: DataFrame
) -> DataFrame:
    df = leftouter_join_on_locationid_and_unix_time(df, known_job_count_df)
    df = add_unix_time_for_known_job_count(df)
    return df


def leftouter_join_on_locationid_and_unix_time(
    df: DataFrame, other_df: DataFrame
) -> DataFrame:
    return df.join(other_df, [IndCqc.location_id, IndCqc.unix_time], "leftouter")


def add_unix_time_for_known_job_count(df: DataFrame) -> DataFrame:
    return df.withColumn(
        IndCqc.filled_posts_unix_time,
        F.when(
            (F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull()),
            F.col(IndCqc.unix_time),
        ).otherwise(F.lit(None)),
    )


def interpolate_values_for_all_dates(df: DataFrame) -> DataFrame:
    df = input_previous_and_next_values_into_df(df)
    df = calculate_interpolated_values_in_new_column(df, IndCqc.interpolation_model)
    return df


def input_previous_and_next_values_into_df(df: DataFrame) -> DataFrame:
    df = get_previous_value_in_column(
        df, IndCqc.ascwds_filled_posts_dedup_clean, IndCqc.previous_filled_posts
    )
    df = get_previous_value_in_column(
        df, IndCqc.filled_posts_unix_time, IndCqc.previous_filled_posts_unix_time
    )
    df = get_next_value_in_new_column(
        df, IndCqc.ascwds_filled_posts_dedup_clean, IndCqc.next_filled_posts
    )
    return get_next_value_in_new_column(
        df, IndCqc.filled_posts_unix_time, IndCqc.next_filled_posts_unix_time
    )


def get_previous_value_in_column(
    df: DataFrame, column_name: str, new_column_name: str
) -> DataFrame:
    return df.withColumn(
        new_column_name,
        F.last(F.col(column_name), ignorenulls=True).over(
            create_window_for_previous_value()
        ),
    )


def create_window_for_previous_value() -> Window:
    return (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(IndCqc.unix_time)
        .rowsBetween(-sys.maxsize, 0)
    )


def get_next_value_in_new_column(
    df: DataFrame, column_name: str, new_column_name: str
) -> DataFrame:
    return df.withColumn(
        new_column_name,
        F.first(F.col(column_name), ignorenulls=True).over(
            create_window_for_next_value()
        ),
    )


def create_window_for_next_value() -> Window:
    return (
        Window.partitionBy(IndCqc.location_id)
        .orderBy(IndCqc.unix_time)
        .rowsBetween(0, sys.maxsize)
    )


def calculate_interpolated_values_in_new_column(
    df: DataFrame, new_column_name: str
) -> DataFrame:
    interpol_udf = F.udf(interpolate_values, FloatType())

    df = df.withColumn(
        new_column_name,
        interpol_udf(
            IndCqc.unix_time,
            IndCqc.previous_filled_posts_unix_time,
            IndCqc.next_filled_posts_unix_time,
            IndCqc.ascwds_filled_posts_dedup_clean,
            IndCqc.previous_filled_posts,
            IndCqc.next_filled_posts,
        ),
    )

    return df.select(IndCqc.location_id, IndCqc.unix_time, IndCqc.interpolation_model)


def interpolate_values(
    unix_time: str,
    previous_job_count_unix_time: str,
    next_job_count_unix_time: str,
    job_count: str,
    previous_job_count: str,
    next_job_count: str,
) -> float:
    if previous_job_count_unix_time == next_job_count_unix_time:
        return job_count
    else:
        job_count_per_unix_time_ratio = (next_job_count - previous_job_count) / (
            next_job_count_unix_time - previous_job_count_unix_time
        )
        return previous_job_count + job_count_per_unix_time_ratio * (
            unix_time - previous_job_count_unix_time
        )
