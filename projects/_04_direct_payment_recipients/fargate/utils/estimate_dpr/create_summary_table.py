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
            - employing_staff
            - employing_self_employed_staff
            - total_dprs_employing_staff
            - pa_filled_posts
    """
    summary_direct_payments_lf = lf.group_by(DP.year_as_integer).agg(
        pl.sum(DP.total_dprs_during_year).cast(pl.Float32).alias(DP.total_dprs),
        pl.sum(DP.service_user_dprs_during_year)
        .cast(pl.Float32)
        .alias(DP.service_user_dprs),
        pl.sum(DP.estimated_service_users_employing_staff)
        .cast(pl.Float32)
        .alias(DP.employing_staff),
        pl.sum(DP.estimated_service_users_employing_self_employed_staff)
        .cast(pl.Float32)
        .alias(DP.employing_self_employed_staff),
        pl.sum(DP.estimated_total_dpr_employing_staff)
        .cast(pl.Float32)
        .alias(DP.total_dprs_employing_staff),
        pl.sum(DP.estimated_pa_filled_posts).cast(pl.Float32).alias(DP.pa_filled_posts),
    )
    return summary_direct_payments_lf
