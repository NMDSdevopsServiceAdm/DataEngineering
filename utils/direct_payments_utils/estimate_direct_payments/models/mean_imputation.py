from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_using_mean(
    direct_payments_df: DataFrame,
) -> DataFrame:
    """
    most_recent_year_df = filter_to_most_recent_year(direct_payments_df)
    mean_proportion_of_service_users_employing_staff = (
        calculate_mean_proportion_of_service_users_employing_staff(most_recent_year_df)
    )

    direct_payments_df = (
        calculate_estimated_service_user_dprs_during_year_employing_staff_using_mean(
            direct_payments_df, mean_proportion_of_service_users_employing_staff
        )
    )
    """
    mean_df = calculate_aggregates_per_year(direct_payments_df)
    mean_df = calculate_mean_per_year(mean_df)

    direct_payments_df = join_mean_into_df(direct_payments_df, mean_df)

    return direct_payments_df


"""
def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    most_recent_year_df = df.where(F.col(DP.YEAR) == Config.MOST_RECENT_YEAR)
    return most_recent_year_df


def calculate_mean_proportion_of_service_users_employing_staff(
    most_recent_year_df: DataFrame,
) -> float:
    mean_proportion_of_service_users_employing_staff = most_recent_year_df.select(
        F.mean(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    ).collect()[0][0]
    return mean_proportion_of_service_users_employing_staff


def calculate_estimated_service_user_dprs_during_year_employing_staff_using_mean(
    direct_payments_df: DataFrame,
    mean_proportion_of_service_users_employing_staff: float,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATE_USING_MEAN,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR)
        * mean_proportion_of_service_users_employing_staff,
    )
    return direct_payments_df
"""


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
