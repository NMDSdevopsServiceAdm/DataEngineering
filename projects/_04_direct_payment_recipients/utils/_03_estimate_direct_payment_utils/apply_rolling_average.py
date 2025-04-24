from pyspark.sql import DataFrame, Window, functions as F

from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def apply_rolling_average(direct_payments_df: DataFrame) -> DataFrame:
    aggregated_df = calculate_aggregates_per_year(direct_payments_df)
    rolling_average_df = create_rolling_average_column(aggregated_df)
    direct_payments_df = join_rolling_average_into_df(
        direct_payments_df, rolling_average_df
    )
    return direct_payments_df


def calculate_aggregates_per_year(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.groupBy(DP.LA_AREA, DP.YEAR_AS_INTEGER).agg(
        F.count(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .cast("integer")
        .alias(DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        F.sum(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(
            DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        ),
    )
    return direct_payments_df


def create_rolling_average_column(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = calculate_rolling_sum(
        direct_payments_df,
        DP.COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.ROLLING_TOTAL_OF_COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    direct_payments_df = calculate_rolling_sum(
        direct_payments_df,
        DP.SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.ROLLING_TOTAL_OF_SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.col(
            DP.ROLLING_TOTAL_OF_SUM_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        )
        / F.col(
            DP.ROLLING_TOTAL_OF_COUNT_OF_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
        ),
    )
    return direct_payments_df


def calculate_rolling_sum(
    df: DataFrame, col_to_sum: str, new_col_name: str
) -> DataFrame:
    df = df.withColumn(
        new_col_name,
        F.sum(col_to_sum).over(define_window_specifications(DP.YEAR_AS_INTEGER)),
    )
    return df


def define_window_specifications(year_column: str) -> Window:
    range = Config.NUMBER_OF_YEARS_ROLLING_AVERAGE - 1
    rolling_window = (
        Window.partitionBy(DP.LA_AREA)
        .orderBy(F.col(year_column).cast("long"))
        .rangeBetween(-(range), 0)
    )
    return rolling_window


def join_rolling_average_into_df(
    direct_payments_df: DataFrame,
    rolling_average_df: DataFrame,
) -> DataFrame:
    rolling_average_df = rolling_average_df.select(
        DP.LA_AREA,
        DP.YEAR_AS_INTEGER,
        DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    direct_payments_df = direct_payments_df.join(
        rolling_average_df, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )
    return direct_payments_df
