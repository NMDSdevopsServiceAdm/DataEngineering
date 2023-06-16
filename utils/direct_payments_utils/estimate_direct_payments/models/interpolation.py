import sys
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
import pyspark.sql

from pyspark.sql.types import ArrayType, LongType, FloatType


# from utils.utils import convert_days_to_year_with_data
#

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_interpolation(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # filter to locations with known service users emlpoying staff
    known_service_users_employing_staff_df = (
        filter_to_locations_with_known_service_users_employing_staff(direct_payments_df)
    )
    # calculate_first_and_last_submission_year_per_la_area
    first_and_last_submission_year_df = (
        calculate_first_and_last_submission_year_per_la_area(
            known_service_users_employing_staff_df
        )
    )

    # convert firts and last known year into time series df
    all_dates_df = convert_first_and_last_known_years_into_exploded_df(
        first_and_last_submission_year_df
    )
    # add known info
    all_dates_df = merge_known_values_with_exploded_dates(
        all_dates_df, known_service_users_employing_staff_df
    )
    # interpolate values for all dates
    all_dates_df = interpolate_values_for_all_dates(all_dates_df)
    # join into df
    direct_payments_df = join_interpolation_into_df(direct_payments_df, all_dates_df)
    return direct_payments_df


"""
def model_interpolation(df: DataFrame) -> DataFrame:

   # known_job_count_df = filter_to_locations_with_a_known_job_count(df)

   # first_and_last_submission_date_df = calculate_first_and_last_submission_date_per_location(known_job_count_df)

   # all_dates_df = convert_first_and_last_known_time_into_timeseries_df(first_and_last_submission_date_df)

   # all_dates_df = add_known_job_count_information(all_dates_df, known_job_count_df)

    all_dates_df = interpolate_values_for_all_dates(all_dates_df)

    df = leftouter_join_on_locationid_and_year_with_data(df, all_dates_df)

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)).otherwise(
            F.col(DP.ESTIMATE_USING_INTERPOLATION)
        ),
    )
    df = update_dataframe_with_identifying_rule(df, DP.ESTIMATE_USING_INTERPOLATION, ESTIMATE_JOB_COUNT)

    return df
"""


def filter_to_locations_with_known_service_users_employing_staff(
    df: DataFrame,
) -> DataFrame:

    df = df.select(
        DP.LA_AREA,
        DP.YEAR_AS_INTEGER,
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
    )
    df = df.where(
        F.col(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF).isNotNull()
    )
    return df


def calculate_first_and_last_submission_year_per_la_area(
    df: DataFrame,
) -> DataFrame:

    df = df.groupBy(DP.LA_AREA).agg(
        F.min(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.FIRST_SUBMISSION_YEAR),
        F.max(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.LAST_SUBMISSION_YEAR),
    )
    return df


def convert_first_and_last_known_years_into_exploded_df(
    df: DataFrame,
) -> DataFrame:
    create_list_of_equally_spaced_points_between_start_and_finish_years_udf = F.udf(
        create_list_of_equally_spaced_points_between_start_and_finish_years,
        ArrayType(LongType()),
    )
    df = df.withColumn(
        DP.INTERPOLATION_YEAR,
        F.explode(
            create_list_of_equally_spaced_points_between_start_and_finish_years_udf(
                DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR
            )
        ),
    )

    df = df.drop(DP.FIRST_SUBMISSION_YEAR, DP.LAST_SUBMISSION_YEAR)
    return df


def create_list_of_equally_spaced_points_between_start_and_finish_years(
    start_year: int, finish_year: int
) -> int:
    year_step = 1
    years = range(int((finish_year - start_year) / year_step) + 1)
    array_of_years = [start_year + year_step * year for year in years]
    return array_of_years


def merge_known_values_with_exploded_dates(
    all_dates_df: DataFrame, known_service_users_employing_staff_df: DataFrame
) -> DataFrame:
    all_dates_df = all_dates_df.withColumnRenamed(
        DP.INTERPOLATION_YEAR, DP.YEAR_AS_INTEGER
    )
    merged_df = all_dates_df.join(
        known_service_users_employing_staff_df, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )

    merged_df = add_year_with_data_for_known_service_users_employing_staff(merged_df)
    return merged_df


def add_year_with_data_for_known_service_users_employing_staff(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
        F.when(
            (
                F.col(
                    DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF
                ).isNotNull()
            ),
            F.col(DP.YEAR_AS_INTEGER),
        ).otherwise(F.lit(None)),
    )
    return df


def interpolate_values_for_all_dates(df: DataFrame) -> DataFrame:
    df = input_previous_and_next_values_into_df(df)
    df = calculated_interpolated_values_in_new_column(
        df, DP.ESTIMATE_USING_INTERPOLATION
    )
    return df


def input_previous_and_next_values_into_df(df: DataFrame) -> DataFrame:
    df = get_previous_value_in_column(
        df,
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
        DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF,
    )
    df = get_previous_value_in_column(
        df,
        DP.SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
        DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
    )
    df = get_next_value_in_new_column(
        df,
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
        DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF,
    )
    df = get_next_value_in_new_column(
        df,
        DP.SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
        DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
    )
    return df


def create_window_for_previous_value() -> Window:
    window = (
        Window.partitionBy(DP.LA_AREA)
        .orderBy(DP.YEAR_AS_INTEGER)
        .rowsBetween(-sys.maxsize, 0)
    )
    return window


def get_previous_value_in_column(
    df: DataFrame, column_name: str, new_column_name: str
) -> DataFrame:
    df = df.withColumn(
        new_column_name,
        F.last(F.col(column_name), ignorenulls=True).over(
            create_window_for_previous_value()
        ),
    )
    return df


def create_window_for_next_value() -> Window:
    window = (
        Window.partitionBy(DP.LA_AREA)
        .orderBy(DP.YEAR_AS_INTEGER)
        .rowsBetween(0, sys.maxsize)
    )
    return window


def get_next_value_in_new_column(
    df: DataFrame, column_name: str, new_column_name: str
) -> DataFrame:
    df = df.withColumn(
        new_column_name,
        F.first(F.col(column_name), ignorenulls=True).over(
            create_window_for_next_value()
        ),
    )
    return df


def calculated_interpolated_values_in_new_column(
    df: DataFrame, new_column_name: str
) -> DataFrame:
    interpolate_udf = F.udf(interpolation_calculation, FloatType())

    df = df.withColumn(
        new_column_name,
        interpolate_udf(
            DP.YEAR_AS_INTEGER,
            DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
            DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF_YEAR_WITH_DATA,
            DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
            DP.PREVIOUS_SERVICE_USERS_EMPLOYING_STAFF,
            DP.NEXT_SERVICE_USERS_EMPLOYING_STAFF,
        ),
    )
    df = df.select(DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATE_USING_INTERPOLATION)
    return df


def interpolation_calculation(
    x: str, x_prev: str, x_next: str, y: str, y_prev: str, y_next: str
) -> float:
    if x_prev == x_next:
        return y
    else:
        m = (y_next - y_prev) / (x_next - x_prev)
        return y_prev + m * (x - x_prev)


def join_interpolation_into_df(
    direct_payments_df: DataFrame,
    interpolation_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.join(
        interpolation_df, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )
    return direct_payments_df
