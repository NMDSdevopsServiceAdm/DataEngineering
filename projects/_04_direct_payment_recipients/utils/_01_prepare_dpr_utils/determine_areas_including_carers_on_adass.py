from pyspark.sql import DataFrame, functions as F

from projects._04_direct_payment_recipients.tests.utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def determine_areas_including_carers_on_adass(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = add_column_with_year_as_integer(direct_payments_df)
    direct_payments_df = calculate_proportion_of_dprs_employing_staff(
        direct_payments_df
    )
    direct_payments_df = calculate_total_dprs_at_year_end(direct_payments_df)
    direct_payments_df = determine_if_adass_base_is_closer_to_total_dpr_or_su_only(
        direct_payments_df
    )
    direct_payments_df = calculate_value_if_adass_base_is_closer_to_total_dpr(
        direct_payments_df
    )
    direct_payments_df = calculate_value_if_adass_base_is_closer_to_su_only(
        direct_payments_df
    )
    direct_payments_df = allocate_proportions(direct_payments_df)
    return direct_payments_df


def add_column_with_year_as_integer(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.YEAR_AS_INTEGER,
        F.col(DP.YEAR).cast("integer"),
    )
    return direct_payments_df


def calculate_proportion_of_dprs_employing_staff(df: DataFrame) -> DataFrame:
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


def determine_if_adass_base_is_closer_to_total_dpr_or_su_only(
    df: DataFrame,
) -> DataFrame:
    df = calculate_difference_between_bases(
        df, DP.DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF, DP.TOTAL_DPRS_AT_YEAR_END
    )
    df = calculate_difference_between_bases(
        df,
        DP.DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF,
        DP.SERVICE_USER_DPRS_AT_YEAR_END,
    )
    df = allocate_which_base_is_closer(df)
    return df


def calculate_difference_between_bases(
    df: DataFrame,
    new_column: str,
    ascof_column: str,
) -> DataFrame:
    df = df.withColumn(
        new_column,
        F.abs(F.col(DP.DPRS_ADASS) - F.col(ascof_column)),
    )
    return df


def allocate_which_base_is_closer(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.CLOSER_BASE,
        F.when(
            F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF)
            < F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF),
            F.lit(Values.TOTAL_DPRS),
        )
        .when(
            F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_TOTAL_ASCOF)
            > F.col(DP.DIFFERENCE_BETWEEN_ADASS_AND_SU_ONLY_ASCOF),
            F.lit(Values.SU_ONLY_DPRS),
        )
        .otherwise(F.lit(Values.TOTAL_DPRS)),
    )
    return df


def calculate_value_if_adass_base_is_closer_to_total_dpr(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_IF_TOTAL_DPR_CLOSER,
        F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF)
        * F.col(DP.TOTAL_DPRS_AT_YEAR_END)
        / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
    )
    return df


def calculate_value_if_adass_base_is_closer_to_su_only(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER,
        (
            (
                F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF)
                * F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END)
            )
            + (F.col(DP.CARER_DPRS_AT_YEAR_END) * Config.CARERS_EMPLOYING_PERCENTAGE)
        )
        / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
    )
    return df


def allocate_proportions(direct_payments_df: DataFrame) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.PROPORTION_ALLOCATED,
        F.when(
            F.col(DP.CLOSER_BASE) == Values.TOTAL_DPRS,
            F.col(DP.PROPORTION_IF_TOTAL_DPR_CLOSER),
        ).when(
            F.col(DP.CLOSER_BASE) == Values.SU_ONLY_DPRS,
            F.col(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER),
        ),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.PROPORTION_ALLOCATED,
        F.when(
            F.col(DP.PROPORTION_ALLOCATED)
            < Config.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_THRESHOLD,
            F.col(DP.PROPORTION_ALLOCATED),
        ).otherwise(F.col(DP.PROPORTION_IF_SERVICE_USER_DPR_CLOSER)),
    )
    direct_payments_df = direct_payments_df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.PROPORTION_IMPORTED).isNotNull(), F.col(DP.PROPORTION_IMPORTED)
        ).otherwise(F.col(DP.PROPORTION_ALLOCATED)),
    )
    return direct_payments_df
