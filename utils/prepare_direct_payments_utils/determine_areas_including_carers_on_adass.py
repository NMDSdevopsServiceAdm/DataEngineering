import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
)

from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

MOST_RECENT_YEAR = 2021
DIFFERENCE_IN_BASES_THRESHOLD = 10.0
PROPORTION_EMPLOYING_STAFF_THRESHOLD = 0.3


def determine_areas_including_carers_on_adass(direct_payments_df: DataFrame) -> DataFrame:
    # TODO
    # filter to most recent year
    most_recent_direct_payments_df = filter_to_most_recent_year(direct_payments_df)
    # calculate_total_dprs_during_year()
    most_recent_direct_payments_df = calculate_total_dprs_during_year(most_recent_direct_payments_df)
    # calculate_dprs_employing_staff()
    most_recent_direct_payments_df = calculate_service_users_employing_staff(most_recent_direct_payments_df)
    # calculate_carers_employing_staff()
    most_recent_direct_payments_df = calculate_carers_employing_staff(most_recent_direct_payments_df)
    # calculate_total_employing_staff_including_carers()
    most_recent_direct_payments_df = calculate_service_users_and_carers_employing_staff(most_recent_direct_payments_df)
    # determine_if_survey_base_is_close_to_ascof_base
    most_recent_direct_payments_df = calculate_difference_between_survey_base_and_total_dpr_during_year(
        most_recent_direct_payments_df
    )
    # alocate_method()
    most_recent_direct_payments_df = allocate_method_for_calculating_service_users_employing_staff(
        most_recent_direct_payments_df
    )
    # calculate_proportion_of_su_employing_staff
    # rejoin to table
    return direct_payments_df


def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    df = df.where(df[DP.YEAR] == MOST_RECENT_YEAR)
    return df


def calculate_total_dprs_during_year(df: DataFrame) -> DataFrame:
    df = df.withColumn(DP.TOTAL_DPRS_DURING_YEAR, df[DP.SERVICE_USER_DPRS_DURING_YEAR] + df[DP.CARER_DPRS_DURING_YEAR])
    return df


def calculate_service_users_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.SERVICE_USERS_EMPLOYING_STAFF,
        df[DP.SERVICE_USER_DPRS_DURING_YEAR] * df[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
    )
    return df


def calculate_carers_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.CARERS_EMPLOYING_STAFF,
        df[DP.CARER_DPRS_DURING_YEAR] * df[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF],
    )
    return df


def calculate_service_users_and_carers_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF,
        df[DP.SERVICE_USERS_EMPLOYING_STAFF] + df[DP.CARERS_EMPLOYING_STAFF],
    )
    return df


def calculate_difference_between_survey_base_and_total_dpr_during_year(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.DIFFERENCE_IN_BASES,
        F.abs(df[DP.DPRS_EMPLOYING_STAFF_ADASS] - df[DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF]),
    )
    return df


def allocate_method_for_calculating_service_users_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.METHOD,
        F.when(
            (
                (df[DP.DIFFERENCE_IN_BASES] < DIFFERENCE_IN_BASES_THRESHOLD)
                | (df[DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF] < PROPORTION_EMPLOYING_STAFF_THRESHOLD)
            ),
            F.lit("adass includes carers"),
        ).otherwise("adass does not include carers"),
    )
    return df
