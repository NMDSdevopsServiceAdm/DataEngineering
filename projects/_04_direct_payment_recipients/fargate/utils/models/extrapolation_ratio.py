import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_extrapolation(direct_payments_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Extrapolates proportion of service users employing staff for years outside the known
    data range.

    For each LA area, identifies the first and last years with known data. For years
    before the first known year or after the last known year, estimates the proportion
    by scaling the boundary data point by how much the mean estimate has moved relative
    to the mean at that boundary year (ratio extrapolation).

    Assumes one row per LA area and year. The result does not depend on row order
    under that assumption, but duplicate LA area/year rows would make the boundary
    values depend on which duplicate comes first.

    Args:
        direct_payments_lf (pl.LazyFrame): Input Polars LazyFrame

    Returns:
        pl.LazyFrame: The input LazyFrame with the following additional columns:
            - first_year_with_data: earliest year with known data per LA area.
            - last_year_with_data: latest year with known data per LA area.
            - estimate_using_extrapolation_ratio: extrapolated proportion for years
                outside the known data range, null for years within the range.
    """
    has_data = pl.col(DP.proportion_employing_staff).is_not_null()

    direct_payments_lf = direct_payments_lf.with_columns(
        pl.col(DP.year_as_integer)
        .filter(has_data)
        .min()
        .cast(pl.Int32)
        .over(DP.la_area)
        .alias(DP.first_year_with_data),
        pl.col(DP.year_as_integer)
        .filter(has_data)
        .max()
        .cast(pl.Int32)
        .over(DP.la_area)
        .alias(DP.last_year_with_data),
    )

    is_first_year = pl.col(DP.year_as_integer) == pl.col(DP.first_year_with_data)
    is_last_year = pl.col(DP.year_as_integer) == pl.col(DP.last_year_with_data)

    first_value = (
        pl.col(DP.proportion_employing_staff)
        .filter(is_first_year)
        .first()
        .over(DP.la_area)
    )
    first_mean = (
        pl.col(DP.estimate_using_mean).filter(is_first_year).first().over(DP.la_area)
    )
    last_value = (
        pl.col(DP.proportion_employing_staff)
        .filter(is_last_year)
        .first()
        .over(DP.la_area)
    )
    last_mean = (
        pl.col(DP.estimate_using_mean).filter(is_last_year).first().over(DP.la_area)
    )

    before_first = pl.col(DP.year_as_integer) < pl.col(DP.first_year_with_data)
    after_last = pl.col(DP.year_as_integer) > pl.col(DP.last_year_with_data)

    mean_ratio_first = pl.col(DP.estimate_using_mean) / first_mean
    mean_ratio_last = pl.col(DP.estimate_using_mean) / last_mean

    return direct_payments_lf.with_columns(
        pl.when(before_first)
        .then(mean_ratio_first * first_value)
        .when(after_last)
        .then(mean_ratio_last * last_value)
        .otherwise(None)
        .alias(DP.estimate_using_extrapolation_ratio)
    )
