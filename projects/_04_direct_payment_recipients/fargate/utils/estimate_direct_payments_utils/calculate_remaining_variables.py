import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.direct_payments_config_polars import (
    DirectPaymentConfiguration as Config,
)


def calculate_remaining_variables(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Derives additional direct payment columns from an input LazyFrame.

    This function calculates several estimated fields related to service users,
    carers, and employment of personal assistants. The calculations are based on
    existing columns in the input LazyFrame and configuration constants.

    The following derived columns are added:
        - estimated_service_users_with_self_employed_staff
        - estimated_total_dpr_employing_staff
        - estimated_total_personal_assistant_filled_posts
        - estimated_proportion_of_total_dpr_employing_staff
        - estimated_proportion_of_dpr_who_are_service_users

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: A new LazyFrame with additional derived columns appended.
    """
    service_users_with_self_employed_staff_expr = (
        pl.col(DP.SERVICE_USER_DPRS_DURING_YEAR)
        * Config.SELF_EMPLOYED_STAFF_PER_SERVICE_USER
    )

    total_dpr_employing_staff_expr = (
        pl.col(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
        + service_users_with_self_employed_staff_expr
    )

    total_personal_assistant_filled_posts_expr = (
        total_dpr_employing_staff_expr * pl.col(DP.FILLED_POSTS_PER_EMPLOYER)
    )

    proportion_of_dpr_employing_staff_expr = total_dpr_employing_staff_expr / pl.col(
        DP.TOTAL_DPRS_DURING_YEAR
    )

    proportion_of_dpr_who_are_service_users_expr = pl.col(
        DP.SERVICE_USER_DPRS_DURING_YEAR
    ) / pl.col(DP.TOTAL_DPRS_DURING_YEAR)

    return lf.with_columns(
        service_users_with_self_employed_staff_expr.alias(
            DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF
        ),
        total_dpr_employing_staff_expr.alias(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF),
        total_personal_assistant_filled_posts_expr.alias(
            DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS
        ),
        proportion_of_dpr_employing_staff_expr.alias(
            DP.ESTIMATED_PROPORTION_OF_TOTAL_DPR_EMPLOYING_STAFF
        ),
        proportion_of_dpr_who_are_service_users_expr.alias(
            DP.ESTIMATED_PROPORTION_OF_DPR_WHO_ARE_SERVICE_USERS
        ),
    )
