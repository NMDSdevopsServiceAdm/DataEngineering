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
    first_value = "first_value"
    first_mean = "first_mean"
    last_value = "last_value"
    last_mean = "last_mean"

    boundaries_lf = (
        direct_payments_lf.filter(
            pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).is_not_null()
        )
        # polars_streaming: groupby-agg-join workaround; could be .min().over() and .max().over() when window functions support streaming
        .group_by(DP.LA_AREA).agg(
            pl.col(DP.YEAR_AS_INTEGER)
            .min()
            .cast(pl.Int32)
            .alias(DP.FIRST_YEAR_WITH_DATA),
            pl.col(DP.YEAR_AS_INTEGER)
            .max()
            .cast(pl.Int32)
            .alias(DP.LAST_YEAR_WITH_DATA),
        )
    )
    direct_payments_lf = direct_payments_lf.join(
        boundaries_lf, on=DP.LA_AREA, how="left"
    )

    first_values_lf = direct_payments_lf.filter(
        pl.col(DP.YEAR_AS_INTEGER) == pl.col(DP.FIRST_YEAR_WITH_DATA)
    ).select(
        DP.LA_AREA,
        pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(first_value),
        pl.col(DP.ESTIMATE_USING_MEAN).alias(first_mean),
    )

    last_values_lf = direct_payments_lf.filter(
        pl.col(DP.YEAR_AS_INTEGER) == pl.col(DP.LAST_YEAR_WITH_DATA)
    ).select(
        DP.LA_AREA,
        pl.col(DP.PROPORTION_OF_SERVICE_USERS_EMPLOYING_STAFF).alias(last_value),
        pl.col(DP.ESTIMATE_USING_MEAN).alias(last_mean),
    )

    direct_payments_lf = direct_payments_lf.join(
        first_values_lf, on=DP.LA_AREA, how="left"
    ).join(last_values_lf, on=DP.LA_AREA, how="left")

    before_first = pl.col(DP.YEAR_AS_INTEGER) < pl.col(DP.FIRST_YEAR_WITH_DATA)
    after_last = pl.col(DP.YEAR_AS_INTEGER) > pl.col(DP.LAST_YEAR_WITH_DATA)

    mean_ratio_first = pl.col(DP.ESTIMATE_USING_MEAN) / pl.col(first_mean)
    mean_ratio_last = pl.col(DP.ESTIMATE_USING_MEAN) / pl.col(last_mean)

    direct_payments_lf = direct_payments_lf.with_columns(
        pl.when(before_first)
        .then(mean_ratio_first * pl.col(first_value))
        .when(after_last)
        .then(mean_ratio_last * pl.col(last_value))
        .otherwise(None)
        .alias(DP.ESTIMATE_USING_EXTRAPOLATION_RATIO)
    )

    return direct_payments_lf.drop(last_mean, first_value, first_mean, last_value)
