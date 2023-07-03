import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def apply_rolling_average(direct_payments_df: DataFrame) -> DataFrame:
    # TODO
    return direct_payments_df


"""
def calculate_rolling_average(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = filter_to_locations_with_known_service_users_employing_staff(direct_payments_df)
    service_users_employing_staff_sum_and_count_df = calculate_aggregates_per_year(populated_df)
    rolling_average_df = create_rolling_average_column(service_users_employing_staff_sum_and_count_df)
    direct_payments_df = join_rolling_average_into_df(direct_payments_df, rolling_average_df)
    return direct_payments_df
"""


"""
def calculate_aggregates_per_year(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.groupBy(DP.YEAR_AS_INTEGER).agg(
        F.count(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
        .cast("integer")
        .alias(DP.COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR),
        F.sum(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF).alias(
            DP.SUM_OF_SERVICE_USER_DPRS_DURING_YEAR
        ),
    )
    return direct_payments_df
"""
"""
def create_rolling_average_column(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = calculate_rolling_sum(
        direct_payments_df,
        DP.COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR,
        DP.ROLLING_TOTAL_COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR,
    )
    direct_payments_df = calculate_rolling_sum(
        direct_payments_df,
        DP.SUM_OF_SERVICE_USER_DPRS_DURING_YEAR,
        DP.ROLLING_TOTAL_SUM_OF_SERVICE_USER_DPRS_DURING_YEAR,
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.ROLLING_AVERAGE,
        F.col(DP.ROLLING_TOTAL_SUM_OF_SERVICE_USER_DPRS_DURING_YEAR)
        / F.col(DP.ROLLING_TOTAL_COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR),
    )
    return direct_payments_df
"""
"""
def calculate_rolling_sum(df: DataFrame, col_to_sum: str, new_col_name: str) -> DataFrame:
    df = df.withColumn(
        new_col_name,
        F.sum(col_to_sum).over(define_window_specifications(DP.YEAR_AS_INTEGER)),
    )
    return df
"""
"""
def define_window_specifications(year_column: str) -> Window:
    rolling_window = (
        Window.partitionBy(F.lit(0))
        .orderBy(F.col(year_column).cast("long"))
        .rangeBetween(-(Config.NUMBER_OF_YEARS_ROLLING_AVERAGE), 0)
    )
    return rolling_window
"""
"""
def join_rolling_average_into_df(
    direct_payments_df: DataFrame,
    rolling_average_df: DataFrame,
) -> DataFrame:
    rolling_average_df = rolling_average_df.select(DP.YEAR_AS_INTEGER, DP.ROLLING_AVERAGE)
    direct_payments_df = direct_payments_df.join(rolling_average_df, [DP.YEAR_AS_INTEGER], "left")
    return direct_payments_df
"""
