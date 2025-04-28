from pyspark.sql import DataFrame, functions as F

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_using_mean(
    direct_payments_df: DataFrame,
) -> DataFrame:
    mean_df = calculate_aggregates_per_year(direct_payments_df)
    mean_df = calculate_mean_per_year(mean_df)
    direct_payments_df = join_mean_into_df(direct_payments_df, mean_df)

    return direct_payments_df


def calculate_aggregates_per_year(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.groupBy(DP.YEAR_AS_INTEGER).agg(
        F.count(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .cast("integer")
        .alias(DP.COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR),
        F.sum(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(
            DP.SUM_OF_SERVICE_USER_DPRS_DURING_YEAR
        ),
    )
    return direct_payments_df


def join_mean_into_df(
    direct_payments_df: DataFrame,
    mean_df: DataFrame,
) -> DataFrame:
    mean_df = mean_df.select(DP.YEAR_AS_INTEGER, DP.ESTIMATE_USING_MEAN)
    direct_payments_df = direct_payments_df.join(mean_df, [DP.YEAR_AS_INTEGER], "left")
    return direct_payments_df


def calculate_mean_per_year(
    mean_df: DataFrame,
) -> DataFrame:
    mean_df = mean_df.withColumn(
        DP.ESTIMATE_USING_MEAN,
        (
            F.col(DP.SUM_OF_SERVICE_USER_DPRS_DURING_YEAR)
            / F.col(DP.COUNT_OF_SERVICE_USER_DPRS_DURING_YEAR)
        ),
    )
    return mean_df
