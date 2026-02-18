from datetime import datetime

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ArchivePartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

most_recent_annual_estimate_date: str = "most_recent_annual_estimate_date"


def select_import_dates_to_archive(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters LazyFrame to only include the most recent monthly estimates, plus historical annual estimates.

    Args:
        lf (pl.LazyFrame): A LazyFrame to archive.

    Returns:
        pl.LazyFrame: A LazyFrame with the most recent monthly estimates, plus historical annual estimates.
    """

    lf = add_latest_annual_estimate_date(lf)

    import_date_col = pl.col(IndCQC.cqc_location_import_date)
    lf = lf.filter(
        (import_date_col >= pl.col(most_recent_annual_estimate_date))
        | (
            (import_date_col < pl.col(most_recent_annual_estimate_date))
            & (
                import_date_col.dt.month()
                == pl.col(most_recent_annual_estimate_date).dt.month()
            )
        )
    )

    return lf.drop(most_recent_annual_estimate_date)


def add_latest_annual_estimate_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a date column with the value 1st April and the year of latest annual estimates publication. For example,
    for estimates published for 2024/25 this will be 2025-04-01.

    Args:
        lf (pl.LazyFrame): A LazyFrame with the column cqc_location_import_date.

    Returns:
        pl.LazyFrame: The input LazyFrame with column for most recent annual estimate date.
    """
    max_date = pl.col(IndCQC.cqc_location_import_date).max()

    april = 4

    lf = lf.with_columns(
        pl.when(max_date.dt.month() < april)
        .then(pl.date(max_date.dt.year() - 1, april, 1))
        .otherwise(pl.date(max_date.dt.year(), april, 1))
        .alias(most_recent_annual_estimate_date)
    )

    return lf


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
    year = date_time.strftime("%Y")
    timestamp = date_time.strftime("%Y-%m-%d %H:%M")
    lf = lf.with_columns(
        (pl.lit(day).alias(ArchiveKeys.archive_day)),
        (pl.lit(month).alias(ArchiveKeys.archive_month)),
        (pl.lit(year).alias(ArchiveKeys.archive_year)),
        (pl.lit(timestamp).alias(ArchiveKeys.archive_timestamp)),
    )

    return lf
    return lf
