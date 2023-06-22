from pyspark.sql import DataFrame


def calculate_remaining_variables(
    direct_payments_df: DataFrame,
) -> DataFrame:
    direct_payments_df = calculate_service_users_with_self_employed_staff(direct_payments_df)
    direct_payments_df = calculate_carers_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_dpr_employing_staff(direct_payments_df)
    direct_payments_df = calculate_total_personal_assistant_filled_posts(direct_payments_df)
    direct_payments_df = calculate_proportion_of_dpr_employing_staff(direct_payments_df)
    return direct_payments_df


def calculate_service_users_with_self_employed_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # su dprs * self employed %
    return direct_payments_df


def calculate_carers_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # carer dprs * carer %
    return direct_payments_df


def calculate_total_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # su employing staff + su with self employed staff + carers employing staff
    return direct_payments_df


def calculate_total_personal_assistant_filled_posts(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # total dpr employing staff * pa ratio
    return direct_payments_df


def calculate_proportion_of_dpr_employing_staff(
    direct_payments_df: DataFrame,
) -> DataFrame:
    # TODO
    # total dpr employing staff/ total dpr
    return direct_payments_df
