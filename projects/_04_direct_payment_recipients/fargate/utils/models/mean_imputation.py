import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_using_mean(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column 'estimate_using_mean' which is the mean
    'proportion_employing_staff' per 'year_as_integer'.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns 'proportion_employing_staff'
            and 'year_as_integer'.

    Returns:
        pl.LazyFrame: A LazyFrame with new column 'estimate_using_mean'.
    """
    mean_expression = pl.mean(DP.proportion_employing_staff).over(DP.year_as_integer)

    return lf.with_columns(mean_expression.alias(DP.estimate_using_mean))
