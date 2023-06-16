import sys
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
import pyspark.sql
from pyspark.sql.types import ArrayType, LongType, FloatType


# from utils.utils import convert_days_to_unix_time
#

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_interpolation(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # filter to locations with known service users emlpoying staff
    known_service_users_employing_staff_df = filter_to_locations_with_a_known_service_users_employing_staff(
        direct_payments_df
    )
    # calculate_first_and_last_submission_year_per_la_area
    # convert firts and last known year into time series df
    # add known info
    # interpolate values for all dates
    # join into df
    return direct_payments_df


def model_interpolation(df: DataFrame) -> DataFrame:

    known_job_count_df = filter_to_locations_with_a_known_job_count(df)

    first_and_last_submission_date_df = calculate_first_and_last_submission_date_per_location(known_job_count_df)

    all_dates_df = convert_first_and_last_known_time_into_timeseries_df(first_and_last_submission_date_df)

    all_dates_df = add_known_job_count_information(all_dates_df, known_job_count_df)

    all_dates_df = interpolate_values_for_all_dates(all_dates_df)

    df = leftouter_join_on_locationid_and_unix_time(df, all_dates_df)

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)).otherwise(
            F.col(DP.ESTIMATE_USING_INTERPOLATION)
        ),
    )
    df = update_dataframe_with_identifying_rule(df, DP.ESTIMATE_USING_INTERPOLATION, ESTIMATE_JOB_COUNT)

    return df


def filter_to_locations_with_a_known_service_users_employing_staff(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    df = df.select(DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
    df = df.where(F.col(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF).isNotNull())
    return df


def calculate_first_and_last_submission_date_per_location(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    return df.groupBy(DP.LA_AREA).agg(
        F.min(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.FIRST_SUBMISSION_YEAR),
        F.max(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.LAST_SUBMISSION_YEAR),
    )


def convert_first_and_last_known_time_into_timeseries_df(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    date_range_udf = F.udf(date_range, ArrayType(LongType()))

    return df.withColumn(
        "unix_time",
        F.explode(date_range_udf(DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR)),
    ).drop(DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR)


def date_range(unix_start_time: int, unix_finish_time: int, step_size_in_days: int = 1) -> int:
    """Return a list of equally spaced points between unix_start_time and unix_finish_time with set stepsizes"""
    unix_time_step = convert_days_to_unix_time(step_size_in_days)

    return [
        unix_start_time + unix_time_step * x
        for x in range(int((unix_finish_time - unix_start_time) / unix_time_step) + 1)
    ]


def add_known_job_count_information(df: DataFrame, known_job_count_df: DataFrame) -> DataFrame:

    df = leftouter_join_on_locationid_and_unix_time(df, known_job_count_df)

    return add_unix_time_for_known_job_count(df)


def leftouter_join_on_locationid_and_unix_time(df: DataFrame, other_df: DataFrame) -> DataFrame:
    return df.join(other_df, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "leftouter")


def add_unix_time_for_known_job_count(df: DataFrame) -> DataFrame:
    return df.withColumn(
        DP.SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
        F.when(
            (F.col(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF).isNotNull()), F.col(DP.YEAR_AS_INTEGER)
        ).otherwise(F.lit(None)),
    )


def window_for_previous_value() -> Window:
    return Window.partitionBy(DP.LA_AREA).orderBy(DP.YEAR_AS_INTEGER).rowsBetween(-sys.maxsize, 0)


def get_previous_value_in_column(df: DataFrame, column_name: str, new_column_name: str) -> DataFrame:
    return df.withColumn(
        new_column_name,
        F.last(F.col(column_name), ignorenulls=True).over(window_for_previous_value()),
    )


def window_for_next_value() -> Window:
    return Window.partitionBy(DP.LA_AREA).orderBy(DP.YEAR_AS_INTEGER).rowsBetween(0, sys.maxsize)


def get_next_value_in_new_column(df: DataFrame, column_name: str, new_column_name: str) -> DataFrame:
    return df.withColumn(
        new_column_name,
        F.first(F.col(column_name), ignorenulls=True).over(window_for_next_value()),
    )


def interpolate_values_for_all_dates(df: DataFrame) -> DataFrame:
    df = input_previous_and_next_values_into_df(df)
    return calculated_interpolated_values_in_new_column(df, DP.ESTIMATE_USING_INTERPOLATION)


def input_previous_and_next_values_into_df(df: DataFrame) -> DataFrame:
    df = get_previous_value_in_column(
        df, DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF
    )
    df = get_previous_value_in_column(
        df, DP.SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA, DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA
    )
    df = get_next_value_in_new_column(
        df, DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF, DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF
    )
    return get_next_value_in_new_column(
        df, DP.SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA, DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA
    )


def calculated_interpolated_values_in_new_column(df: DataFrame, new_column_name: str) -> DataFrame:
    interpol_udf = F.udf(interpolation_calculation, FloatType())

    df = df.withColumn(
        new_column_name,
        interpol_udf(
            DP.YEAR_AS_INTEGER,
            DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
            DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
            DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF,
            DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF,
        ),
    )

    return df.select(DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATE_USING_INTERPOLATION)


def interpolation_calculation(x: str, x_prev: str, x_next: str, y: str, y_prev: str, y_next: str) -> float:
    if x_prev == x_next:
        return y
    else:
        m = (y_next - y_prev) / (x_next - x_prev)
        return y_prev + m * (x - x_prev)
