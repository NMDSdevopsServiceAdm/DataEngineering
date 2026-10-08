import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
from projects._04_direct_payment_recipients.direct_payments_config import (
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
        - estimated_service_users_employing_self_employed_staff
        - estimated_total_dpr_employing_staff
        - estimated_pa_filled_posts
        - estimated_proportion_of_total_dpr_employing_staff

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: A new LazyFrame with additional derived columns appended.
    """
    employing_self_employed_staff_expr = (
        pl.col(DP.service_user_dprs_during_year)
        * Config.SELF_EMPLOYED_STAFF_PER_SERVICE_USER
    )

    total_dpr_employing_staff_expr = (
        pl.col(DP.estimated_service_users_employing_staff)
        + employing_self_employed_staff_expr
    )

    pa_filled_posts_expr = total_dpr_employing_staff_expr * pl.col(
        DP.filled_posts_per_employer
    )

    proportion_of_dpr_employing_staff_expr = total_dpr_employing_staff_expr / pl.col(
        DP.total_dprs_during_year
    )

    return lf.with_columns(
        employing_self_employed_staff_expr.alias(
            DP.estimated_service_users_employing_self_employed_staff
        ),
        total_dpr_employing_staff_expr.alias(DP.estimated_total_dpr_employing_staff),
        pa_filled_posts_expr.alias(DP.estimated_pa_filled_posts),
        proportion_of_dpr_employing_staff_expr.alias(
            DP.estimated_proportion_of_total_dpr_employing_staff
        ),
    )
