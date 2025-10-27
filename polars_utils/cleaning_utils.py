import polars as pl

import_date_s3_uri_format = "%Y%m%d"


def add_aligned_date_column(
    primary_lf: pl.LazyFrame,
    secondary_lf: pl.LazyFrame,
    primary_column: str,
    secondary_column: str,
) -> pl.LazyFrame:
    """
    Adds a column to `primary_lf` containing the closest past matching date from `secondary_lf`.

    Uses `join_asof` to align each primary date with the nearest non-future secondary date.

    After this function, we join the full secondary_lf into primary_lf based on the
    `secondary_column`. Unfortunately this needs to be a two step process as we want the entire
    datasets to be aligned on the same date, as opposed to aligning dates to whenever each
    individual location ID or postcode last existed.

    Args:
        primary_lf (pl.LazyFrame): LazyFrame to which the aligned date column will be added.
        secondary_lf (pl.LazyFrame): LazyFrame with dates for alignment.
        primary_column (str): Column name in `primary_lf` containing date values.
        secondary_column (str): Column name in `secondary_lf` containing date values.

    Returns:
        pl.LazyFrame: The original `primary_lf` with an additional column containing
        aligned dates from `secondary_lf`.
    """
    # Ensure both frames are sorted by date for join_asof
    primary_sorted = primary_lf.sort(primary_column)
    secondary_sorted = (
        secondary_lf.select(secondary_column).unique().sort(secondary_column)
    )

    # Join secondary dates to primary dates using asof join
    primary_lf_with_aligned_dates = primary_sorted.join_asof(
        secondary_sorted,
        left_on=primary_column,
        right_on=secondary_column,
        strategy="backward",  # less than or equal to
        coalesce=False,  # include secondary_column in join
    )

    return primary_lf_with_aligned_dates


def column_to_date(
    lf: pl.LazyFrame,
    column_to_format: str,
    new_column: str = None,
    string_format: str = import_date_s3_uri_format,
) -> pl.LazyFrame:
    """
    Converts a single column of strings to dates.

    Strings in column_to_format can be formatted as either 20250101 or 2025-01-01. The conversion will overwrite the
    column_to_format unless new_column is provided.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns that contain dates in string format.
        column_to_format (str): The column name with dates as strings.
        new_column (str): Optional. A new column name for the converted strings.
        string_format (str): Defaults to "%Y%m%d".

    Returns:
        pl.LazyFrame: The input LazyFrame with strings converted to dates.
    """
    if new_column is None:
        new_column = column_to_format

    lf = lf.with_columns(
        pl.col(column_to_format)
        .cast(pl.String())
        .str.replace_all("-", "")
        .str.to_date(string_format)
        .alias(new_column)
    )

    return lf
