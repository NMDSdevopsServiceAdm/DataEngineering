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

    Args:
        direct_payments_lf (pl.LazyFrame): Input Polars LazyFrame

    Returns:
        pl.LazyFrame: The input LazyFrame with the following additional columns:
            - first_year_with_data: earliest year with known data per LA area.
            - last_year_with_data: latest year with known data per LA area.
            - estimate_using_extrapolation_ratio: extrapolated proportion for years
                outside the known data range, null for years within the range.
    """
    has_data = pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).is_not_null()

    direct_payments_lf = direct_payments_lf.with_columns(
        pl.col(DP.YEAR_AS_INTEGER)
        .filter(has_data)
        .min()
        .cast(pl.Int32)
        .over(DP.LA_AREA)
        .alias(DP.FIRST_YEAR_WITH_DATA),
        pl.col(DP.YEAR_AS_INTEGER)
        .filter(has_data)
        .max()
        .cast(pl.Int32)
        .over(DP.LA_AREA)
        .alias(DP.LAST_YEAR_WITH_DATA),
    )

    is_first_year = pl.col(DP.YEAR_AS_INTEGER) == pl.col(DP.FIRST_YEAR_WITH_DATA)
    is_last_year = pl.col(DP.YEAR_AS_INTEGER) == pl.col(DP.LAST_YEAR_WITH_DATA)

    first_value = (
        pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .filter(is_first_year)
        .first()
        .over(DP.LA_AREA)
    )
    first_mean = pl.col(DP.ESTIMATE_USING_MEAN).filter(is_first_year).first().over(DP.LA_AREA)
    last_value = (
        pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF)
        .filter(is_last_year)
        .first()
        .over(DP.LA_AREA)
    )
    last_mean = pl.col(DP.ESTIMATE_USING_MEAN).filter(is_last_year).first().over(DP.LA_AREA)

    before_first = pl.col(DP.YEAR_AS_INTEGER) < pl.col(DP.FIRST_YEAR_WITH_DATA)
    after_last = pl.col(DP.YEAR_AS_INTEGER) > pl.col(DP.LAST_YEAR_WITH_DATA)

    mean_ratio_first = pl.col(DP.ESTIMATE_USING_MEAN) / first_mean
    mean_ratio_last = pl.col(DP.ESTIMATE_USING_MEAN) / last_mean

    return direct_payments_lf.with_columns(
        pl.when(before_first)
        .then(mean_ratio_first * first_value)
        .when(after_last)
        .then(mean_ratio_last * last_value)
        .otherwise(None)
        .alias(DP.ESTIMATE_USING_EXTRAPOLATION_RATIO)
    )
