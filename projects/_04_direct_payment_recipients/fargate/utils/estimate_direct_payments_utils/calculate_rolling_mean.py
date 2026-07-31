import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def calculate_rolling_mean(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates a rolling mean over the current row and previous two rows within
    each local authority. Years are ordered by YEAR_AS_INTEGER, but do not need
    to be consecutive.

    All years in all areas are assumed to be populated in the column
    'ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF'.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'LA_AREA', 'YEAR_AS_INTEGER' and
            'ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF'.

    Returns:
        pl.LazyFrame: A LazyFrame with new column
            'ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF'.
    """
    grouping_cols = [DP.LA_AREA, DP.YEAR_AS_INTEGER]

    lf = lf.sort(grouping_cols)

    lf = lf.with_columns(
        pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .rolling_mean(window_size=3, min_samples=1)
        .over(DP.LA_AREA)
        .alias(DP.ROLLING_AVERAGE_ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
    )

    return lf
