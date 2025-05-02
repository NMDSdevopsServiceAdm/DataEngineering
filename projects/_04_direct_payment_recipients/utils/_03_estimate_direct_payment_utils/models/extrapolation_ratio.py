from pyspark.sql import DataFrame, functions as F

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_extrapolation(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = add_columns_with_first_and_last_years_of_data(
        direct_payments_df
    )
    direct_payments_df = add_data_point_from_given_year_of_data(
        direct_payments_df,
        DP.FIRST_YEAR_WITH_DATA,
        DP.ESTIMATE_USING_MEAN,
        DP.FIRST_YEAR_MEAN_ESTIMATE,
    )
    direct_payments_df = add_data_point_from_given_year_of_data(
        direct_payments_df,
        DP.FIRST_YEAR_WITH_DATA,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.FIRST_DATA_POINT,
    )
    direct_payments_df = add_data_point_from_given_year_of_data(
        direct_payments_df,
        DP.LAST_YEAR_WITH_DATA,
        DP.ESTIMATE_USING_MEAN,
        DP.LAST_YEAR_MEAN_ESTIMATE,
    )
    direct_payments_df = add_data_point_from_given_year_of_data(
        direct_payments_df,
        DP.LAST_YEAR_WITH_DATA,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.LAST_DATA_POINT,
    )
    ratio_df = calculate_extrapolation_ratios(direct_payments_df)
    extrapolation_df = calculate_extrapolation_estimates(ratio_df)

    direct_payments_df = join_extrapolation_into_df(
        direct_payments_df, extrapolation_df
    )

    return direct_payments_df


def add_columns_with_first_and_last_years_of_data(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = filter_to_locations_with_known_service_users_employing_staff(
        direct_payments_df
    )
    first_and_last_submission_date_df = determine_first_and_last_years_with_data(
        populated_df
    )

    direct_payments_df = direct_payments_df.join(
        first_and_last_submission_date_df, DP.LA_AREA, "left"
    )

    return direct_payments_df


def add_data_point_from_given_year_of_data(
    direct_payments_df: DataFrame,
    year_of_data_to_add: str,
    original_column: str,
    new_column: str,
) -> DataFrame:
    df = direct_payments_df.where(
        F.col(year_of_data_to_add) == F.col(DP.YEAR_AS_INTEGER)
    )
    df = df.withColumnRenamed(original_column, new_column)
    df = df.select(DP.LA_AREA, new_column)

    direct_payments_df = direct_payments_df.join(df, [DP.LA_AREA], "left")

    return direct_payments_df


def calculate_extrapolation_ratios(
    direct_payments_df: DataFrame,
) -> DataFrame:
    ratio_df = direct_payments_df.where(
        (F.col(DP.YEAR_AS_INTEGER) < F.col(DP.FIRST_YEAR_WITH_DATA))
        | (F.col(DP.YEAR_AS_INTEGER) > F.col(DP.LAST_YEAR_WITH_DATA))
    )
    ratio_df = ratio_df.withColumn(
        DP.EXTRAPOLATION_RATIO,
        F.when(
            (F.col(DP.YEAR_AS_INTEGER) < F.col(DP.FIRST_YEAR_WITH_DATA)),
            (F.col(DP.ESTIMATE_USING_MEAN) / F.col(DP.FIRST_YEAR_MEAN_ESTIMATE)),
        ).when(
            (F.col(DP.YEAR_AS_INTEGER) > F.col(DP.LAST_YEAR_WITH_DATA)),
            (F.col(DP.ESTIMATE_USING_MEAN) / F.col(DP.LAST_YEAR_MEAN_ESTIMATE)),
        ),
    )

    return ratio_df


def calculate_extrapolation_estimates(
    ratio_df: DataFrame,
) -> DataFrame:
    extrapolation_df = ratio_df.withColumn(
        DP.ESTIMATE_USING_EXTRAPOLATION_RATIO,
        F.when(
            (F.col(DP.YEAR_AS_INTEGER) < F.col(DP.FIRST_YEAR_WITH_DATA)),
            (F.col(DP.FIRST_DATA_POINT) * F.col(DP.EXTRAPOLATION_RATIO)),
        ).when(
            (F.col(DP.YEAR_AS_INTEGER) > F.col(DP.LAST_YEAR_WITH_DATA)),
            (F.col(DP.LAST_DATA_POINT) * F.col(DP.EXTRAPOLATION_RATIO)),
        ),
    )
    return extrapolation_df


def filter_to_locations_with_known_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = direct_payments_df.where(
        F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull()
    )
    return populated_df


def determine_first_and_last_years_with_data(
    populated_df: DataFrame,
) -> DataFrame:
    first_and_last_submission_date_df = populated_df.groupBy(DP.LA_AREA).agg(
        F.min(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.FIRST_YEAR_WITH_DATA),
        F.max(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.LAST_YEAR_WITH_DATA),
    )
    return first_and_last_submission_date_df


def join_extrapolation_into_df(
    direct_payments_df: DataFrame,
    extrapolation_df: DataFrame,
) -> DataFrame:
    extrapolation_df = extrapolation_df.select(
        DP.LA_AREA, DP.YEAR_AS_INTEGER, DP.ESTIMATE_USING_EXTRAPOLATION_RATIO
    )
    direct_payments_df = direct_payments_df.join(
        extrapolation_df, [DP.LA_AREA, DP.YEAR_AS_INTEGER], "left"
    )
    return direct_payments_df
