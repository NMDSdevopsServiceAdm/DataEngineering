import sys
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
import pyspark.sql

from utils.utils import convert_days_to_unix_time
from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    UNIX_TIME,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
)
from pyspark.sql.types import ArrayType, LongType, FloatType
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

FIRST_SUBMISSION_TIME = "first_submission_time"
LAST_SUBMISSION_TIME = "last_submission_time"
PREVIOUS_JOB_COUNT = "previous_job_count"
NEXT_JOB_COUNT = "next_job_count"
JOB_COUNT_UNIX_TIME = "job_count_unix_time"
PREVIOUS_JOB_COUNT_UNIX_TIME = "previous_job_count_unix_time"
NEXT_JOB_COUNT_UNIX_TIME = "next_job_count_unix_time"
INTERPOLATION_MODEL = "interpolation_model"


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
        ESTIMATE_JOB_COUNT,
        F.when(
            F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
        ).otherwise(F.col(INTERPOLATION_MODEL)),
    )
    df = update_dataframe_with_identifying_rule(
        df, INTERPOLATION_MODEL, ESTIMATE_JOB_COUNT
    )

    return df


def filter_to_locations_with_a_known_job_count(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.select(LOCATION_ID, UNIX_TIME, JOB_COUNT)

    return df.where(F.col(JOB_COUNT).isNotNull())


def calculate_first_and_last_submission_date_per_location(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    return df.groupBy(LOCATION_ID).agg(
        F.min(UNIX_TIME).cast("integer").alias(FIRST_SUBMISSION_TIME),
        F.max(UNIX_TIME).cast("integer").alias(LAST_SUBMISSION_TIME),
    )


def convert_first_and_last_known_years_into_exploded_df(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    date_range_udf = F.udf(create_date_range, ArrayType(LongType()))

    return df.withColumn(
        "unix_time",
        F.explode(date_range_udf(FIRST_SUBMISSION_TIME, LAST_SUBMISSION_TIME)),
    ).drop(FIRST_SUBMISSION_TIME, LAST_SUBMISSION_TIME)


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
    return df.join(other_df, [LOCATION_ID, UNIX_TIME], "leftouter")


def add_unix_time_for_known_job_count(df: DataFrame) -> DataFrame:
    return df.withColumn(
        JOB_COUNT_UNIX_TIME,
        F.when((F.col(JOB_COUNT).isNotNull()), F.col(UNIX_TIME)).otherwise(F.lit(None)),
    )


def create_window_for_previous_value() -> Window:
    return (
        Window.partitionBy(LOCATION_ID).orderBy(UNIX_TIME).rowsBetween(-sys.maxsize, 0)
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


def create_window_for_next_value() -> Window:
    return (
        Window.partitionBy(LOCATION_ID).orderBy(UNIX_TIME).rowsBetween(0, sys.maxsize)
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


def interpolate_values_for_all_dates(df: DataFrame) -> DataFrame:
    df = input_previous_and_next_values_into_df(df)
    df = calculated_interpolated_values_in_new_column(df, INTERPOLATION_MODEL)
    return df


def input_previous_and_next_values_into_df(df: DataFrame) -> DataFrame:
    df = get_previous_value_in_column(df, JOB_COUNT, PREVIOUS_JOB_COUNT)
    df = get_previous_value_in_column(
        df, JOB_COUNT_UNIX_TIME, PREVIOUS_JOB_COUNT_UNIX_TIME
    )
    df = get_next_value_in_new_column(df, JOB_COUNT, NEXT_JOB_COUNT)
    return get_next_value_in_new_column(
        df, JOB_COUNT_UNIX_TIME, NEXT_JOB_COUNT_UNIX_TIME
    )


def calculated_interpolated_values_in_new_column(
    df: DataFrame, new_column_name: str
) -> DataFrame:
    interpol_udf = F.udf(interpolation_calculation, FloatType())

    df = df.withColumn(
        new_column_name,
        interpol_udf(
            UNIX_TIME,
            PREVIOUS_JOB_COUNT_UNIX_TIME,
            NEXT_JOB_COUNT_UNIX_TIME,
            JOB_COUNT,
            PREVIOUS_JOB_COUNT,
            NEXT_JOB_COUNT,
        ),
    )

    return df.select(LOCATION_ID, UNIX_TIME, INTERPOLATION_MODEL)


def interpolation_calculation(
    x: str, x_prev: str, x_next: str, y: str, y_prev: str, y_next: str
) -> float:
    if x_prev == x_next:
        return y
    else:
        m = (y_next - y_prev) / (x_next - x_prev)
        return y_prev + m * (x - x_prev)
