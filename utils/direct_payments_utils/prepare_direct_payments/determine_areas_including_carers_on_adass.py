import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)

from utils.direct_payments_utils.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
)


def determine_areas_including_carers_on_adass(
    direct_payments_df: DataFrame,
) -> DataFrame:

    direct_payments_df = calculate_propoartion_of_dprs_employing_staff(direct_payments_df)
    most_recent_direct_payments_df = filter_to_most_recent_year(direct_payments_df)
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

    enriched_direct_payments_df = remove_outliers(enriched_direct_payments_df)
    return enriched_direct_payments_df


def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    df = df.where(F.col(DP.YEAR) == Config.MOST_RECENT_YEAR)
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
        F.col(DP.CARER_DPRS_AT_YEAR_END) * Config.CARERS_EMPLOYING_PERCENTAGE,
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
    """
    ### Replace allocation logic after estimates script is created ###
    1) If ADASS DPRs is more than 100 greater than ascof total dprs, remove data point
    2) Work out if the number of DPR's ADASS have surveyed is closer to the ASCOF service user figure or to the total of SU and carers.
    3) If it's closer to ASCOF service user, then do (% employing from survey * SU's from ASCOF) + (carers * magic low % figure)
    4) If it's closer to the total ASCOF figure, then do % employing from survey * total from ASCOF
    5) Then divide each of these figures by SU's from ASCOF
    """

    df = df.withColumn(
        DP.METHOD,
        F.when(
            (
                (F.col(DP.DIFFERENCE_IN_BASES) < Config.DIFFERENCE_IN_BASES_THRESHOLD)
                | (F.col(DP.PROPORTION_OF_DPR_EMPLOYING_STAFF) < Config.PROPORTION_EMPLOYING_STAFF_THRESHOLD)
            ),
            F.lit(Values.ADASS_INCLUDES_CARERS),
        ).otherwise(Values.ADASS_DOES_NOT_INCLUDE_CARERS),
    )
    return df


def calculate_proportion_of_service_users_only_employing_staff(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP,
        F.when(
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull(),
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        )
        .when(
            (F.col(DP.METHOD) == Values.ADASS_INCLUDES_CARERS),
            F.col(DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END) / F.col(DP.SERVICE_USER_DPRS_AT_YEAR_END),
        )
        .when(
            (F.col(DP.METHOD) == Values.ADASS_DOES_NOT_INCLUDE_CARERS),
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
        DP.TOTAL_DPRS_AT_YEAR_END,
        DP.SERVICE_USERS_EMPLOYING_STAFF_AT_YEAR_END,
        DP.CARERS_EMPLOYING_STAFF_AT_YEAR_END,
        DP.SERVICE_USERS_AND_CARERS_EMPLOYING_STAFF_AT_YEAR_END,
        DP.DIFFERENCE_IN_BASES,
        DP.METHOD,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP,
    )
    new_df = direct_payments_df.join(most_recent_direct_payments_df, on=[DP.LA_AREA, DP.YEAR], how="left")
    new_df = new_df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP).isNotNull(),
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP),
        ).otherwise(F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)),
    )
    new_df = new_df.drop(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF_TEMP)
    return new_df


def remove_outliers(df: DataFrame) -> DataFrame:
    # TODO
    # calculate average proportion employing staff for each LA over all years
    # remove any values that are more than a threshold value away from the average
    return df
