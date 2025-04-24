from pyspark.sql import DataFrame, functions as F

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
    DirectPaymentColumnValues as Values,
)
from projects._04_direct_payment_recipients.direct_payments_configuration import (
    DirectPaymentConfiguration as Config,
    DirectPaymentsOutlierThresholds as OT,
)


def remove_outliers(df: DataFrame) -> DataFrame:
    df = create_column_to_mark_outliers_for_removal(df)
    df = identify_values_below_zero_or_above_one(df)
    df = identify_extreme_values_when_only_value_in_la_area(df)
    df = calculate_mean_proportion_of_service_users_employing_staff(df)
    df = identify_outliers_using_threshold_value(df)
    df = identify_extreme_values_not_following_a_trend_in_most_recent_year(df)
    df = retain_cases_where_latest_number_we_know_is_not_outlier(df)
    df = remove_identified_outliers(df)
    return df


def create_column_to_mark_outliers_for_removal(df: DataFrame) -> DataFrame:
    df = df.withColumn(DP.OUTLIERS_FOR_REMOVAL, F.lit(Values.RETAIN))
    return df


def identify_values_below_zero_or_above_one(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (
                F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
                > OT.ONE_HUNDRED_PERCENT
            )
            | (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < OT.ZERO_PERCENT),
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def identify_extreme_values_when_only_value_in_la_area(df: DataFrame) -> DataFrame:
    count_df = df.groupBy(DP.LA_AREA).agg(
        F.count(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .cast("integer")
        .alias(DP.COUNT_OF_YEARS_WITH_PROPORTION),
    )
    count_df = count_df.select(DP.LA_AREA, DP.COUNT_OF_YEARS_WITH_PROPORTION)

    df = df.join(count_df, on=DP.LA_AREA, how="left")
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.COUNT_OF_YEARS_WITH_PROPORTION) == 1)
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())
            & (
                (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 0.85)
                | (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.15)
            ),
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def calculate_mean_proportion_of_service_users_employing_staff(
    df: DataFrame,
) -> DataFrame:
    means_df = df.groupBy(DP.LA_AREA).mean(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
    )
    df = df.join(means_df, on=DP.LA_AREA, how="left")
    df = df.withColumnRenamed(
        DP.GROUPED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
    )
    return df


def identify_outliers_using_threshold_value(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            F.abs(
                F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
                - F.col(DP.MEAN_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
            )
            >= Config.ADASS_PROPORTION_OUTLIER_THRESHOLD,
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def identify_extreme_values_not_following_a_trend_in_most_recent_year(
    df: DataFrame,
) -> DataFrame:
    filtered_df = df.where(F.col(DP.YEAR_AS_INTEGER) == 2021)
    filtered_df = filtered_df.withColumn(
        DP.PENULTIMATE_YEAR_DATA, F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    )
    filtered_df = filtered_df.select(DP.LA_AREA, DP.PENULTIMATE_YEAR_DATA)
    df = df.join(filtered_df, on=DP.LA_AREA, how="left")
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.YEAR_AS_INTEGER) == 2022)
            & (F.col(DP.PENULTIMATE_YEAR_DATA).isNotNull())
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())
            & (
                (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 0.9)
                | (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.1)
            )
            & (
                F.abs(
                    F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
                    - F.col(DP.PENULTIMATE_YEAR_DATA)
                )
                > 0.3
            ),
            F.lit(Values.REMOVE),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def retain_cases_where_latest_number_we_know_is_not_outlier(df: DataFrame) -> DataFrame:
    df = add_column_with_last_year_of_data(df, DP.LAST_YEAR_CONTAINING_RAW_DATA)
    df = add_data_point_from_given_year_of_data(
        df,
        DP.LAST_YEAR_CONTAINING_RAW_DATA,
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        DP.LAST_RAW_DATA_POINT,
    )
    df = df.withColumn(
        DP.OUTLIERS_FOR_REMOVAL,
        F.when(
            (F.col(DP.YEAR_AS_INTEGER) == F.col(DP.LAST_YEAR_CONTAINING_RAW_DATA))
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) > 0.25)
            & (F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF) < 0.75),
            F.lit(Values.RETAIN),
        ).otherwise(F.col(DP.OUTLIERS_FOR_REMOVAL)),
    )
    return df


def add_column_with_last_year_of_data(
    direct_payments_df: DataFrame,
    column_name: str,
) -> DataFrame:
    populated_df = filter_to_locations_with_known_service_users_employing_staff(
        direct_payments_df
    )
    last_submission_date_df = determine_last_year_with_data(populated_df, column_name)

    direct_payments_df = direct_payments_df.join(
        last_submission_date_df, DP.LA_AREA, "left"
    )

    return direct_payments_df


def filter_to_locations_with_known_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = direct_payments_df.where(
        F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull()
    )
    return populated_df


def determine_last_year_with_data(
    populated_df: DataFrame,
    column_name: str,
) -> DataFrame:
    last_submission_date_df = populated_df.groupBy(DP.LA_AREA).agg(
        F.max(DP.YEAR_AS_INTEGER).cast("integer").alias(column_name),
    )
    return last_submission_date_df


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


def remove_identified_outliers(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF,
        F.when(
            F.col(DP.OUTLIERS_FOR_REMOVAL) == Values.RETAIN,
            F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF),
        ).otherwise(F.lit(None)),
    )
    return df
