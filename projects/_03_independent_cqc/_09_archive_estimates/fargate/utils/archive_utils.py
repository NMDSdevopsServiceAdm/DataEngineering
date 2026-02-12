from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TempColumns:
    max_import_date_month = "max_import_date_month"
    max_import_date_year = "max_import_date_year"
    most_recent_annual_estimate_date = "most_recent_annual_estimate_date"


def select_import_dates_to_archive(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters dataframe to only include the most recent monthly estimates, plus historical annual estimates.

    Args:
        lf (pl.LazyFrame): A LazyFrame to archive.

    Returns:
        pl.LazyFrame: A LazyFrame with the most recent monthly estimates, plus historical annual estimates.
    """

    lf = add_a_column_with_the_date_of_most_recent_annual_estimates(lf)

    import_date_col = pl.col(IndCQC.cqc_location_import_date)
    most_recent_annual_estimate_date_col = pl.col(
        TempColumns.most_recent_annual_estimate_date
    )
    lf = lf.filter(
        (import_date_col >= most_recent_annual_estimate_date_col)
        | (
            (import_date_col < most_recent_annual_estimate_date_col)
            & (
                import_date_col.dt.month()
                == most_recent_annual_estimate_date_col.dt.month()
            )
        )
    )

    return lf


def add_a_column_with_the_date_of_most_recent_annual_estimates(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Adds a date column with the value 1st April and the year of latest annual estimates publication. For example,
    for estimates published for 2024/25 this will be 2025-04-01.

    Args:
        lf (pl.LazyFrame): A LazyFrame with the column cqc_location_import_date.

    Returns:
        pl.LazyFrame: The input LazyFrame with column for most recent annual estimate date.
    """
    expr_max_import_date = pl.col(IndCQC.cqc_location_import_date).max()
    lf = lf.with_columns(
        expr_max_import_date.dt.month().alias(TempColumns.max_import_date_month),
        expr_max_import_date.dt.year().alias(TempColumns.max_import_date_year),
    )

    march = 3
    april = 4
    first_of_month = 1
    lf = lf.with_columns(
        pl.when(pl.col(TempColumns.max_import_date_month) <= march)
        .then(date(pl.col(TempColumns.max_import_date_year) - 1, april, first_of_month))
        .otherwise(date(TempColumns.max_import_date_year, april, first_of_month))
        .alias(TempColumns.most_recent_annual_estimate_date)
    )

    return lf
