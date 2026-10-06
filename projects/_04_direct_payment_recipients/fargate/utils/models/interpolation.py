import polars as pl

from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)


def model_interpolation(
    direct_payments_lf: pl.LazyFrame,
    col_with_nulls: str,
) -> pl.LazyFrame:
    """
    Performs straight line interpolation of missing values for the
    imputed proportion of service users employing staff.

    Args:
        direct_payments_lf (pl.LazyFrame): Input LazyFrame with columns la_area,
            year_as_integer and proportion_employing_staff
        col_with_nulls (str): A column with null values to interpolate between.

    Returns:
        pl.LazyFrame: Original LazyFrame with an additional column
            estimate_using_interpolation
    """
    return direct_payments_lf.with_columns(
        pl.col(col_with_nulls)
        .interpolate_by(DP.year_as_integer)
        .over(DP.la_area)
        .alias(DP.estimate_using_interpolation)
    )
