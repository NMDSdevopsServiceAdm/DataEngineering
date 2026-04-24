import polars as pl

from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_interpolation(
    direct_payments_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Performs straight line interpolation of missing values for the
    estimated proportion of service users employing staff.

    Args:
        direct_payments_lf (pl.LazyFrame): Input LazyFrame with columns LA_AREA,
            YEAR_AS_INTEGER and ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF

    Returns:
        pl.LazyFrame: Original LazyFrame with an additional column
            ESTIMATE_USING_INTERPOLATION
    """
    return direct_payments_lf.with_columns(
        pl.col(DP.ESTIMATED_PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .interpolate_by(DP.YEAR_AS_INTEGER)
        .over(DP.LA_AREA)
        .alias(DP.ESTIMATE_USING_INTERPOLATION)
    )
