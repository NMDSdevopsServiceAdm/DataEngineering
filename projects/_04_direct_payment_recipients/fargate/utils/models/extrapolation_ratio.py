import polars as pl
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_extrapolation(direct_payments_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Extrapolates proportion of service users employing staff for years outside the known data range.

    For each LA area, identifies the first and last years with known data. For years
    before the first known year or after the last known year, estimates the proportion
    by scaling the boundary data point by how much the mean estimate has moved relative
    to the mean at that boundary year (ratio extrapolation).

    Args:
        direct_payments_lf: Polars LazyFrame containing direct payments data with columns for
            LA area, year, proportion of service users employing staff, and mean estimates.

    Returns:
        The input LazyFrame with the following additional columns:
            - first_year_with_data: earliest year with known data per LA area.
            - last_year_with_data: latest year with known data per LA area.
            - estimate_using_extrapolation_ratio: extrapolated proportion for years
                outside the known data range, null for years within the range.
    """
    has_data = pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).is_not_null()

    first_year = (
        pl.when(has_data)
        .then(pl.col(DP.YEAR_AS_INTEGER))
        .min()
        .over(DP.LA_AREA)
        .cast(pl.Int32)
        .alias(DP.FIRST_YEAR_WITH_DATA)
    )
    last_year = (
        pl.when(has_data)
        .then(pl.col(DP.YEAR_AS_INTEGER))
        .max()
        .over(DP.LA_AREA)
        .cast(pl.Int32)
        .alias(DP.LAST_YEAR_WITH_DATA)
    )

    before_first = pl.col(DP.YEAR_AS_INTEGER) < pl.col(DP.FIRST_YEAR_WITH_DATA)
    after_last = pl.col(DP.YEAR_AS_INTEGER) > pl.col(DP.LAST_YEAR_WITH_DATA)

    extrapolation_ratio = (
        pl.when(before_first)
        .then(
            pl.col(DP.ESTIMATE_USING_MEAN)
            / value_at_boundary_year(DP.ESTIMATE_USING_MEAN, DP.FIRST_YEAR_WITH_DATA)
        )
        .when(after_last)
        .then(
            pl.col(DP.ESTIMATE_USING_MEAN)
            / value_at_boundary_year(DP.ESTIMATE_USING_MEAN, DP.LAST_YEAR_WITH_DATA)
        )
    )

    data_point = (
        pl.when(before_first)
        .then(
            value_at_boundary_year(
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, DP.FIRST_YEAR_WITH_DATA
            )
        )
        .when(after_last)
        .then(
            value_at_boundary_year(
                DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF, DP.LAST_YEAR_WITH_DATA
            )
        )
    )

    return direct_payments_lf.with_columns(first_year, last_year).with_columns(
        (extrapolation_ratio * data_point).alias(DP.ESTIMATE_USING_EXTRAPOLATION_RATIO)
    )


def value_at_boundary_year(value_col: str, boundary_year_col: str) -> pl.Expr:
    """
    Returns the value of a column at the boundary year, broadcast across the LA area group.

    Args:
        value_col: Name of the column to retrieve the value from.
        boundary_year_col: Name of the column holding the boundary year to match against.

    Returns:
        A Polars expression resolving to the value of `value_col` at the boundary year
        for each row in the LA area group.
    """
    return (
        pl.when(pl.col(DP.YEAR_AS_INTEGER) == pl.col(boundary_year_col))
        .then(pl.col(value_col))
        .max()
        .over(DP.LA_AREA)
    )
