import polars as pl


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
    lf: pl.LazyFrame, column: str, new_column: str = None
) -> pl.LazyFrame:
    """
    Converts a string or integer column (YYYYmmDD or YYYY-mm-DD) to a Polars Date column.

    The conversion will overwrite the original `column` unless `new_column` is provided.

    Args:
        lf (pl.LazyFrame): Input Polars LazyFrame.
        column (str): Column name to convert.
        new_column (str): Optional. If None, overwrites the original column.

    Returns:
        pl.LazyFrame: LazyFrame with the converted date column.
    """
    string_format = "%Y%m%d"
    target_col = new_column or column

    return lf.with_columns(
        pl.col(column)
        .cast(pl.Utf8)
        .str.replace_all("-", "")
        .str.to_date(string_format)
        .alias(target_col)
    )
