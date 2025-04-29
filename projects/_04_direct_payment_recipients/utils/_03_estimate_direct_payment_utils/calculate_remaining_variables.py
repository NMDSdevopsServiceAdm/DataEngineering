from pyspark.sql import DataFrame, functions as F

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def calculate_remaining_variables(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = calculate_service_users_with_self_employed_staff(
        direct_payments_df
    )
    direct_payments_df = calculate_carers_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_dpr_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_personal_assistant_filled_posts(
        direct_payments_df
    )
    direct_payments_df = calculate_proportion_of_dpr_employing_staff(direct_payments_df)
    direct_payments_df = calculate_proportion_of_dpr_who_are_service_users(
        direct_payments_df
    )
    return direct_payments_df


def calculate_service_users_with_self_employed_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR)
        * Config.SELF_EMPLOYED_STAFF_PER_SERVICE_USER,
    )
    return direct_payments_df


def calculate_carers_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_CARERS_EMPLOYING_STAFF,
        F.col(DP.CARER_DPRS_DURING_YEAR) * Config.CARERS_EMPLOYING_PERCENTAGE,
    )
    return direct_payments_df


def calculate_total_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF,
        F.col(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
        + F.col(DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF)
        + F.col(DP.ESTIMATED_CARERS_EMPLOYING_STAFF),
    )
    return direct_payments_df


def calculate_total_personal_assistant_filled_posts(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS,
        F.col(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF)
        * F.col(DP.FILLED_POSTS_PER_EMPLOYER),
    )
    return direct_payments_df


def calculate_proportion_of_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF,
        F.col(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF)
        / F.col(DP.TOTAL_DPRS_DURING_YEAR),
    )
    return direct_payments_df


def calculate_proportion_of_dpr_who_are_service_users(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) / F.col(DP.TOTAL_DPRS_DURING_YEAR),
    )
    return direct_payments_df
