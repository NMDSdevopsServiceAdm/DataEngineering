import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def calculate_rolling_mean(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates a rolling mean over the current row and previous two rows within
    each local authority. Years are ordered by year_as_integer, but do not need
    to be consecutive.

    All years in all areas are assumed to be populated in the column
    'imputed_proportion_employing_staff'.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'la_area', 'year_as_integer' and
            'imputed_proportion_employing_staff'.

    Returns:
        pl.LazyFrame: A LazyFrame with new column
            'rolling_average_proportion_employing_staff'.
    """
    grouping_cols = [DP.la_area, DP.year_as_integer]

    lf = lf.sort(grouping_cols)

    lf = lf.with_columns(
        pl.col(DP.imputed_proportion_employing_staff)
        .rolling_mean(window_size=3, min_samples=1)
        .over(DP.la_area)
        .alias(DP.rolling_average_proportion_employing_staff)
    )

    return lf
