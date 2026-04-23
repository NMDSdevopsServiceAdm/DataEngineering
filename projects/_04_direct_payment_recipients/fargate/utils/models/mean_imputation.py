import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_using_mean(lf: pl.LazyFrame) -> pl.LazyFrame:
    mean_expression = pl.mean(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).over(
        DP.YEAR_AS_INTEGER
    )

    lf = lf.with_columns(
        mean_expression.alias(DP.ESTIMATE_USING_MEAN),
    )

    return lf
