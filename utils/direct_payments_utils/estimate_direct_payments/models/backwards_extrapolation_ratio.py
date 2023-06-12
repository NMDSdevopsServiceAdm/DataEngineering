from pyspark.sql import DataFrame


def model_extrapolation_backwards(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = add_column_with_year_as_integer(direct_payments_df)
    direct_payments_df = add_column_with_first_year_of_data(direct_payments_df)
    direct_payments_df = add_column_with_percentage_service_users_employing_staff_in_first_year_of_data(
        direct_payments_df
    )
    direct_payments_df = calculate_extrapolation_ratio_for_earlier_years(direct_payments_df)
    direct_payments_df = calculate_ratio_estimates(direct_payments_df)

    return direct_payments_df


def add_column_with_year_as_integer(
    direct_payments_df: DataFrame,
) -> DataFrame:
    return direct_payments_df


def add_column_with_first_year_of_data(
    direct_payments_df: DataFrame,
) -> DataFrame:
    return direct_payments_df


def add_column_with_percentage_service_users_employing_staff_in_first_year_of_data(
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
