from pyspark.sql import DataFrame, Window
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
    direct_payments_df = add_data_point_from_first_year_of_data(
        direct_payments_df, DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, DP.FIRST_DATA_POINT
    )
    direct_payments_df = calculate_rolling_average(direct_payments_df)
    direct_payments_df = add_data_point_from_first_year_of_data(
        direct_payments_df, DP.ROLLING_AVERAGE, DP.FIRST_YEAR_ROLLING_AVERAGE
    )
    direct_payments_df = calculate_extrapolation_ratio_for_earlier_years(direct_payments_df)
    direct_payments_df = calculate_ratio_estimates(direct_payments_df, DP.FIRST_DATA_POINT)

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
    populated_df = filter_to_locations_with_known_proportion_service_users_employing_staff(direct_payments_df)
    first_and_last_submission_date_df = determine_first_year_with_data(populated_df)

    direct_payments_df = direct_payments_df.join(first_and_last_submission_date_df, DP.LA_AREA, "left")

    return direct_payments_df


def calculate_rolling_average(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = filter_to_locations_with_known_proportion_service_users_employing_staff(direct_payments_df)
    proportion_service_users_employing_staff_sum_and_count_df = calculate_proportion_aggregates_per_year(populated_df)
    rolling_average_df = create_rolling_average_column(proportion_service_users_employing_staff_sum_and_count_df)
    direct_payments_df = join_rolling_average_into_df(direct_payments_df, rolling_average_df)
    return direct_payments_df


def add_data_point_from_first_year_of_data(
    direct_payments_df: DataFrame,
    original_column: str,
    new_column: str,
) -> DataFrame:
    first_year_df = direct_payments_df.where(F.col(DP.FIRST_YEAR_WITH_DATA) == F.col(DP.YEAR_AS_INTEGER))
    first_year_df = first_year_df.withColumnRenamed(original_column, new_column)
    first_year_df = first_year_df.select(DP.LA_AREA, new_column)

    direct_payments_df = direct_payments_df.join(first_year_df, [DP.LA_AREA], "left")

    return direct_payments_df


def calculate_extrapolation_ratio_for_earlier_years(
    direct_payments_df: DataFrame,
) -> DataFrame:
    relevant_data_df = direct_payments_df.where(F.col(DP.YEAR_AS_INTEGER) < F.col(DP.FIRST_YEAR_WITH_DATA))
    direct_payments_df.withColumn(
        DP.EXTRAPOLATION_RATIO,
        (1 + (F.col(DP.ROLLING_AVERAGE) - F.col(DP.FIRST_YEAR_ROLLING_AVERAGE)) / F.col(DP.FIRST_YEAR_ROLLING_AVERAGE)),
    )
    return relevant_data_df


def calculate_extrapolation_estimates(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.withColumn(
        DP.ESTIMATE_USING_BACKWARD_EXTRAPOLATION_RATIO,
        (F.col(DP.FIRST_YEAR_WITH_DATA) * F.col(DP.EXTRAPOLATION_RATIO)),
    )
    return direct_payments_df


def filter_to_locations_with_known_proportion_service_users_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    populated_df = direct_payments_df.where(F.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).isNotNull())
    return populated_df


def calculate_proportion_aggregates_per_year(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = direct_payments_df.groupBy(DP.YEAR_AS_INTEGER).agg(
        F.count(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).cast("integer").alias(DP.COUNT_OF_PROPORTION),
        F.sum(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(DP.SUM_OF_PROPORTION),
    )
    return direct_payments_df


def create_rolling_average_column(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = calculate_rolling_sum(
        direct_payments_df,
        DP.COUNT_OF_PROPORTION,
        DP.ROLLING_TOTAL_COUNT_OF_PROPORTION,
    )
    direct_payments_df = calculate_rolling_sum(
        direct_payments_df,
        DP.SUM_OF_PROPORTION,
        DP.ROLLING_TOTAL_SUM_OF_PROPORTION,
    )

    direct_payments_df = direct_payments_df.withColumn(
        DP.ROLLING_AVERAGE,
        F.col(DP.ROLLING_TOTAL_SUM_OF_PROPORTION) / F.col(DP.ROLLING_TOTAL_COUNT_OF_PROPORTION),
    )
    direct_payments_df.select(DP.YEAR_AS_INTEGER, DP.ROLLING_AVERAGE).show()
    return direct_payments_df


def calculate_rolling_sum(df: DataFrame, col_to_sum: str, new_col_name: str) -> DataFrame:
    df = df.withColumn(
        new_col_name,
        F.sum(col_to_sum).over(define_window_specifications(DP.YEAR_AS_INTEGER)),
    )
    return df


def define_window_specifications(year_column: str) -> Window:
    rolling_window = Window.orderBy(F.col(year_column).cast("long")).rangeBetween(
        -(Config.NUMBER_OF_YEARS_ROLLING_AVERAGE), 0
    )
    return rolling_window


def join_rolling_average_into_df(
    direct_payments_df: DataFrame,
    rolling_average_df: DataFrame,
) -> DataFrame:
    rolling_average_df = rolling_average_df.select(DP.YEAR_AS_INTEGER, DP.ROLLING_AVERAGE)
    direct_payments_df = direct_payments_df.join(rolling_average_df, [DP.YEAR_AS_INTEGER], "left")
    return direct_payments_df


def determine_first_year_with_data(
    populated_df: DataFrame,
) -> DataFrame:
    first_and_last_submission_date_df = populated_df.groupBy(DP.LA_AREA).agg(
        F.min(DP.YEAR_AS_INTEGER).cast("integer").alias(DP.FIRST_YEAR_WITH_DATA),
    )
    return first_and_last_submission_date_df
