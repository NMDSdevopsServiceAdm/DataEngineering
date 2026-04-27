import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def calculate_rolling_mean(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates the rolling mean of
    'ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF' over a three year
    period from current year and two previous years.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'LA_AREA', 'YEAR_AS_INTEGER' and
            'ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF'.

    Returns:
        pl.LazyFrame: A LazyFrame with new column
            'ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF'.
    """
    grouping_cols = [DP.LA_AREA, DP.YEAR_AS_INTEGER]

    rolling_mean_lf = (
        lf.sort(grouping_cols)
        .rolling(index_column=DP.YEAR_AS_INTEGER, group_by=DP.LA_AREA, period="3i")
        .agg(
            pl.mean(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(
                DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF
            )
        )
    )

    return lf.join(rolling_mean_lf, on=grouping_cols, how="left")
