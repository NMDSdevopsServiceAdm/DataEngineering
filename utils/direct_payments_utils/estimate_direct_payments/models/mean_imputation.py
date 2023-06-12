from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def model_when_no_historical_data(
    direct_payments_df: DataFrame,
) -> DataFrame:
    most_recent_year_df = filter_to_most_recent_year(direct_payments_df)
    mean_proportion_of_service_users_employing_staff = calculate_mean_proportion_of_service_users_employing_staff(
        most_recent_year_df
    )
    direct_payments_df = calculate_estimated_service_user_dprs_during_year_employing_staff_using_mean(
        direct_payments_df, mean_proportion_of_service_users_employing_staff
    )
    return direct_payments_df


def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    most_recent_year_df = df.where(F.col(DP.YEAR) == Config.MOST_RECENT_YEAR)
    return most_recent_year_df


def calculate_mean_proportion_of_service_users_employing_staff(most_recent_year_df: DataFrame) -> float:
    mean_proportion_of_service_users_employing_staff = most_recent_year_df.select(
        F.mean(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    ).collect()[0][0]
    return mean_proportion_of_service_users_employing_staff


def calculate_estimated_service_user_dprs_during_year_employing_staff_using_mean(
    direct_payments_df: DataFrame, mean_proportion_of_service_users_employing_staff: float
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) * mean_proportion_of_service_users_employing_staff,
    )
    return direct_payments_df
