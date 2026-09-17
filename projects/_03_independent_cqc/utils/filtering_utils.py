import polars as pl

from polars_utils import utils


def get_matched_ascwds_dates(metadata_source: str, date_col: str) -> pl.Series:
    """Return the distinct ASCWDS import dates present in metadata_source.

    metadata_source's date_col already reflects the CQC-side asof match to ASCWDS
    import dates, so returning its distinct values keeps callers aligned with the
    dates _01_merge.py's join trusts, rather than independently deriving a retention
    rule that can disagree with it.

    Args:
        metadata_source (str): path to the metadata dataset.
        date_col (str): name of the date column to return distinct matched dates for.

    Returns:
        pl.Series: distinct, non-null dates present in date_col.
    """
    matched_dates_lf = (
        utils.scan_parquet(metadata_source).select(date_col).unique().drop_nulls()
    )

    return matched_dates_lf.collect().to_series()
