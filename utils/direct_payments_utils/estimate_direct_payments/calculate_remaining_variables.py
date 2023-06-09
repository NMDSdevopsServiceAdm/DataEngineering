from pyspark.sql import DataFrame


def calculate_remaining_variables(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    direct_payments_df = calculate_service_users_with_self_employed_staff(
        direct_payments_df
    )
    direct_payments_df = calculate_carers_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_dpr_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_personal_assistant_filled_posts(
        direct_payments_df
    )
    direct_payments_df = calculate_proportion_of_dpr_employing_staff(direct_payments_df)
    return direct_payments_df


def calculate_service_users_with_self_employed_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO

    return direct_payments_df


def calculate_carers_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO

    return direct_payments_df


def calculate_total_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO

    return direct_payments_df


def calculate_total_personal_assistant_filled_posts(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO

    return direct_payments_df


def calculate_proportion_of_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO

    return direct_payments_df
