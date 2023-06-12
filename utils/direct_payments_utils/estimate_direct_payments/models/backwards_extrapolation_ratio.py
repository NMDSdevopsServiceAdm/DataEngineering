from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def model_extrapolation_backwards(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = add_column_with_year_as_integer(direct_payments_df)
    direct_payments_df = add_column_with_first_year_of_data(direct_payments_df)
    direct_payments_df = add_column_with_percentage_service_users_employing_staff_in_first_year_of_data(
        direct_payments_df
    )
    direct_payments_df = calculate_rolling_average(direct_payments_df)
    direct_payments_df = calculate_extrapolation_ratio_for_earlier_years(direct_payments_df)
    direct_payments_df = calculate_ratio_estimates(direct_payments_df)

    return direct_payments_df


def add_column_with_year_as_integer(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.YEAR_AS_INTEGER,
        F.col(DP.YEAR).cast("int"),
    )
    return direct_payments_df


def add_column_with_first_year_of_data(
    direct_payments_df: DataFrame,
) -> DataFrame:

    populated_df = direct_payments_df.where(F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())

    first_and_last_submission_date_df = populated_df.groupBy(DP.LA_AREA).agg(
        F.min(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.FIRST_YEAR_WITH_DATA),
    )

    direct_payments_df = direct_payments_df.join(first_and_last_submission_date_df, DP.LA_AREA, "left")

    return direct_payments_df

    return direct_payments_df


def add_column_with_percentage_service_users_employing_staff_in_first_year_of_data(
    direct_payments_df: DataFrame,
) -> DataFrame:
    return direct_payments_df


def calculate_rolling_average(
    direct_payments_df: DataFrame,
) -> DataFrame:
    return direct_payments_df


def calculate_extrapolation_ratio_for_earlier_years(
    direct_payments_df: DataFrame,
) -> DataFrame:
    return direct_payments_df


def calculate_ratio_estimates(
    direct_payments_df: DataFrame,
) -> DataFrame:
    return direct_payments_df
