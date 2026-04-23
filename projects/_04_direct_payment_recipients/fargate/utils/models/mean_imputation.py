import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_using_mean(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column 'ESTIMATE_USING_MEAN' which is the mean
    'PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF' per 'YEAR_AS_INTEGER'.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF' and 'YEAR_AS_INTEGER'.

    Returns:
        pl.LazyFrame: A LazyFrame with new column 'ESTIMATE_USING_MEAN'.
    """
    mean_expression = pl.mean(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).over(
        DP.YEAR_AS_INTEGER
    )

    return lf.with_columns(
        mean_expression.alias(DP.ESTIMATE_USING_MEAN),
    )
