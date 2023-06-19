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

    direct_payments_df = calculate_proportion_of_dprs_employing_staff(direct_payments_df)
    #  most_recent_direct_payments_df = filter_to_most_recent_year(direct_payments_df)
    most_recent_direct_payments_df = calculate_total_dprs_at_year_end(most_recent_direct_payments_df)
    # TODO: function which adds column to say if adass base is closer to ascof total dprs or su dprs

    # TODO: function to create formula if adass includes everyone

    # TODO: function to create formula if adass includes su only

    # TODO: function to select method - comapare bases and look if % is over 100

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
    """    enriched_direct_payments_df = rejoin_new_variables_into_direct_payments_data(
        direct_payments_df, most_recent_direct_payments_df
    )"""

    enriched_direct_payments_df = remove_outliers(enriched_direct_payments_df)
    return enriched_direct_payments_df


"""
def filter_to_most_recent_year(df: DataFrame) -> DataFrame:
    df = df.where(F.col(DP.YEAR) == Config.MOST_RECENT_YEAR)
    return df
"""


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


def determine_if_adass_base_is_closer_to_total_dpr_or_su_only(df: DataFrame) -> DataFrame:
    # TODO
    # Calculate abs diff between adass base and total dpr
    # calculate abs diff between adass base and SU only dpr
    # allocate which value is closer
    return df


def calculate_value_if_adass_base_is_closer_to_total_dpr(df: DataFrame) -> DataFrame:
    # TODO
    return df


def calculate_value_if_adass_base_is_closer_to_su_only(df: DataFrame) -> DataFrame:
    # TODO
    return df


def allocate_proportions(df: DataFrame) -> DataFrame:
    # TODO
    # If closer to total, apply total
    # If closer to su only, apply su only

    # If proportion is > 100%, apply su only

    # Merge calculated and precalculated proportions
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
    1 - If we think ADASS have surveyed everyone, which is the correct thing to do, we see this by the amount they've surveyed being closer to the total DPR's than to just service users.
    2 - If this results in us saying more than 100% of service users employ staff, then we apply % employing staff from the survey to service users only and add on 0.6% of carers employing staff (method 1). Then we divide that by number of service users.
    3 - If the amount surveyed is closer to the total DPR's then we apply % employing staff from the survey to total DPR's (method 2). Then we divide that by number of service users.
    4 - If the amount surveyed is closer to the service users figure then we apply % employing staff from the survey to service users only and add on 0.6% of carers employing staff (method 1). Then we divide that by number of service users.


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
    df = calculate_mean_proportion_of_service_users_employing_staff(df)
    # remove any values that are more than a threshold value away from the average
    df = remove_outliers_using_threshold_value(df)
    return df


def calculate_mean_proportion_of_service_users_employing_staff(
    df: DataFrame,
) -> DataFrame:
    means_df = df.groupBy(DP.LA_AREA).mean(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    df = df.join(means_df, on=DP.LA_AREA, how="left")
    df = df.withColumnRenamed(
        DP.GROUPED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    return df


def remove_outliers_using_threshold_value(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.abs(
                F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
                - F.col(DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
            )
            < Config.ADASS_PROPORTION_OUTLIER_THRESHOLD,
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ),
    )
    return df
