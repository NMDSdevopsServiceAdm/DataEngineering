from datetime import datetime

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import ArchiveColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def select_import_dates_to_archive(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters LazyFrame to only include the most recent monthly estimates, plus historical annual estimates.

    Args:
        lf (pl.LazyFrame): A LazyFrame to archive.

    Returns:
        pl.LazyFrame: A LazyFrame with the most recent monthly estimates, plus historical annual estimates.
    """

    lf = add_column_with_the_date_of_most_recent_annual_estimates(lf)

    import_date_col = pl.col(IndCQC.cqc_location_import_date)
    most_recent_annual_estimate_date_col = pl.col(
        ArchiveColumns.most_recent_annual_estimate_date
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

    return lf.drop(ArchiveColumns.most_recent_annual_estimate_date)


def add_column_with_the_date_of_most_recent_annual_estimates(
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
        expr_max_import_date.dt.month().alias(ArchiveColumns.max_import_date_month),
        expr_max_import_date.dt.year().alias(ArchiveColumns.max_import_date_year),
    )

    march = 3
    april = 4
    first_of_month = 1
    lf = lf.with_columns(
        pl.when(pl.col(ArchiveColumns.max_import_date_month) <= march)
        .then(
            pl.date(
                pl.col(ArchiveColumns.max_import_date_year) - 1, april, first_of_month
            )
        )
        .otherwise(pl.date(ArchiveColumns.max_import_date_year, april, first_of_month))
        .alias(ArchiveColumns.most_recent_annual_estimate_date)
    )

    return lf.drop(
        [ArchiveColumns.max_import_date_month, ArchiveColumns.max_import_date_year]
    )


def create_archive_date_partition_columns(
    lf: pl.LazyFrame, date_time: datetime
) -> pl.LazyFrame:
    """
    Adds columns for archive day, month, year and timestamp using the given datetime.

    Args:
        lf(pl.LazyFrame): A LazyFrame with a data column.
        date_time(datetime): A date time to be used to construct the archive partition columns.

    Returns:
        pl.LazyFrame: A LazyFrame with archive day, month, and year columns added.
    """
    day = date_time.strftime("%d")
    month = date_time.strftime("%m")
    year = str(date_time.year)
    timestamp = str(date_time)[:16]
    lf = lf.with_columns(
        (pl.lit(day).alias(ArchiveKeys.archive_day)),
        (pl.lit(month).alias(ArchiveKeys.archive_month)),
        (pl.lit(year).alias(ArchiveKeys.archive_year)),
        (pl.lit(timestamp).alias(ArchiveKeys.archive_timestamp)),
    )

    return lf
