import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def create_summary_table(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Aggregates the data by year, summing all estimated and actual counts
    across all LA areas to produce a national summary table.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: A LazyFrame grouped by year with the following columns:
            - total_dprs
            - service_user_dprs
            - service_users_employing_staff
            - service_users_with_self_employed_staff
            - total_dprs_employing_staff
            - total_personal_assistant_filled_posts
    """
    summary_direct_payments_lf = lf.group_by(DP.YEAR_AS_INTEGER).agg(
        pl.sum(DP.TOTAL_DPRS_DURING_YEAR).cast(pl.Float32).alias(DP.TOTAL_DPRS),
        pl.sum(DP.SERVICE_USER_DPRS_DURING_YEAR)
        .cast(pl.Float32)
        .alias(DP.SERVICE_USER_DPRS),
        pl.sum(DP.ESTIMATED_SERVICE_USER_DPRS_DURING_YEAR_EMPLOYING_STAFF)
        .cast(pl.Float32)
        .alias(DP.SERVICE_USERS_EMPLOYING_STAFF),
        pl.sum(DP.ESTIMATED_SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF)
        .cast(pl.Float32)
        .alias(DP.SERVICE_USERS_WITH_SELF_EMPLOYED_STAFF),
        pl.sum(DP.ESTIMATED_TOTAL_DPR_EMPLOYING_STAFF)
        .cast(pl.Float32)
        .alias(DP.TOTAL_DPRS_EMPLOYING_STAFF),
        pl.sum(DP.ESTIMATED_TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS)
        .cast(pl.Float32)
        .alias(DP.TOTAL_PERSONAL_ASSISTANT_FILLED_POSTS),
    )
    return summary_direct_payments_lf
