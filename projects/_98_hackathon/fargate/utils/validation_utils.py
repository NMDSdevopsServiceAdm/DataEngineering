import polars as pl

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def has_one_import_date_per_month(df: pl.DataFrame) -> bool:
    """Checks each calendar month has at most one import date.

    Args:
        df (pl.DataFrame): Worker turnover data with `ascwds_worker_import_date`.

    Returns:
        bool: True if no month has more than one distinct import date.
    """
    import_dates = df.get_column(AWKClean.ascwds_worker_import_date)
    return import_dates.n_unique() == import_dates.dt.truncate("1mo").n_unique()
