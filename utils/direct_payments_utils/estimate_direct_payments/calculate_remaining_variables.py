from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def calculate_remaining_variables(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = calculate_service_users_with_self_employed_staff(direct_payments_df)
    direct_payments_df = calculate_carers_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_dpr_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_personal_assistant_filled_posts(direct_payments_df)
    direct_payments_df = calculate_proportion_of_dpr_employing_staff(direct_payments_df)
    return direct_payments_df


def calculate_service_users_with_self_employed_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # su dprs * self employed %
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF,
        F.col(DP.SERVICE_USER_DPRS_DURING_YEAR) * Config.SELF_EMPLOYED_STAFF_PER_SERVICE_USER,
    )
    return direct_payments_df


def calculate_carers_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # carer dprs * carer %
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATED_CARERS_EMPLOYING_STAFF,
        F.col(DP.CARER_DPRS_DURING_YEAR) * Config.CARERS_EMPLOYING_PERCENTAGE,
    )
    return direct_payments_df


def calculate_total_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # su employing staff + su with self employed staff + carers employing staff
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
    # TODO
    # join pa ratio to df
    # total dpr employing staff * pa ratio
    return direct_payments_df


def calculate_proportion_of_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # total dpr employing staff/ total dpr
    return direct_payments_df
