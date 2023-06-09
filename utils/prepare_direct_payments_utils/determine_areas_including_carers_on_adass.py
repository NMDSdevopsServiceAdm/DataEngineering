import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.prepare_direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)

MOST_RECENT_YEAR = 2021
# The carer's employing percentage was calculated from a question in older surveys. As this is so close to zero it was removed as a question from more recent surveys and we use the most recent value.
CARERS_EMPLOYING_PERCENTAGE = 0.0063872289536592
DIFFERENCE_IN_BASES_THRESHOLD = 100.0
PROPORTION_EMPLOYING_STAFF_THRESHOLD = 0.1
ADASS_INCLUDES_CARERS = "adass includes carers"
ADASS_DOES_NOT_INCLUDE_CARERS = "addas does not include carers"


def determine_areas_including_carers_on_adass(
    direct_payments_df: DataFrame,
) -> DataFrame:
    most_recent_direct_payments_df = filter_to_most_recent_year(direct_payments_df)
    most_recent_direct_payments_df = calculate_propoartion_of_dprs_employing_staff(most_recent_direct_payments_df)
    most_recent_direct_payments_df = calculate_total_dprs_at_year_end(most_recent_direct_payments_df)
    most_recent_direct_payments_df = calculate_service_users_employing_staff(most_recent_direct_payments_df)
    most_recent_direct_payments_df = calculate_carers_employing_staff(most_recent_direct_payments_df)
    most_recent_direct_payments_df = calculate_service_users_and_carers_employing_staff(most_recent_direct_payments_df)
    most_recent_direct_payments_df = calculate_difference_between_survey_base_and_total_dpr_at_year_end(
        most_recent_direct_payments_df
    )
    most_recent_direct_payments_df = allocate_method_for_calculating_service_users_employing_staff(
        most_recent_direct_payments_df
    )
    most_recent_direct_payments_df = calculate_proportion_of_service_users_only_employing_staff(
        most_recent_direct_payments_df
    )
    enriched_direct_payments_df = rejoin_new_variables_into_direct_payments_data(
        direct_payments_df, most_recent_direct_payments_df
    )
    return enriched_direct_payments_df


def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    df = df.where(F.col(DP.YEAR) == MOST_RECENT_YEAR)
    return df


def calculate_propoartion_of_dprs_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_OF_DPR_EMPLOYING_STAFF,
        F.col(DP.DPRS_EMPLOYING_STAFF_ADASS) / F.col(DP.DPRS_ADASS),
    )
    return df


def calculate_total_dprs_at_year_end(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.TOTAL_DPRS_AT_YEAR_END,
        F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END) + F.col(DP.CARER_DPRS_AT_YEAR_END),
    )
    return df


def calculate_service_users_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END,
        F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END) * F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF),
    )
    return df


def calculate_carers_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END,
        F.col(DP.CARER_DPRS_AT_YEAR_END) * CARERS_EMPLOYING_PERCENTAGE,
    )
    return df


def calculate_service_users_and_carers_employing_staff(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END,
        F.col(DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END) + F.col(DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END),
    )
    return df


def calculate_difference_between_survey_base_and_total_dpr_at_year_end(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.DIFFERENCE_IN_BASES,
        F.abs(F.col(DP.DPRS_EMPLOYING_STAFF_ADASS) - F.col(DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END)),
    )
    return df


def allocate_method_for_calculating_service_users_employing_staff(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.METHOD,
        F.when(
            (
                (F.col(DP.DIFFERENCE_IN_BASES) < DIFFERENCE_IN_BASES_THRESHOLD)
                | (F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF) < PROPORTION_EMPLOYING_STAFF_THRESHOLD)
            ),
            F.lit(ADASS_INCLUDES_CARERS),
        ).otherwise(ADASS_DOES_NOT_INCLUDE_CARERS),
    )
    return df


def calculate_proportion_of_service_users_only_employing_staff(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            (F.col(DP.METHOD) == ADASS_INCLUDES_CARERS),
            F.col(DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END) / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
        )
        .when(
            (F.col(DP.METHOD) == ADASS_DOES_NOT_INCLUDE_CARERS),
            F.col(DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END) / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
        )
        .otherwise(F.lit(None)),
    )
    return df


def rejoin_new_variables_into_direct_payments_data(
    direct_payments_df: DataFrame,
    most_recent_direct_payments_df: DataFrame,
) -> DataFrame:
    most_recent_direct_payments_df = most_recent_direct_payments_df.select(
        DP.LA_AREA,
        DP.YEAR,
        DP.PROPORTION_OF_DPR_EMPLOYING_STAFF,
        DP.TOTAL_DPRS_AT_YEAR_END,
        DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END,
        DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END,
        DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END,
        DP.DIFFERENCE_IN_BASES,
        DP.METHOD,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    new_df = direct_payments_df.join(most_recent_direct_payments_df, on=[DP.LA_AREA, DP.YEAR], how="left")
    return new_df
