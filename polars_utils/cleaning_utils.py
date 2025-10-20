import polars as pl


def add_aligned_date_column(
    primary_df: pl.DataFrame,
    secondary_df: pl.DataFrame,
    primary_column: str,
    secondary_column: str,
) -> pl.DataFrame:
    """
    Adds a column to `primary_df` containing the closest matching date from `secondary_df`.

    This function aligns dates between two DataFrames by determining the
    nearest non-future match in `secondary_df` for each date in `primary_df`,
    then joins the result back to the primary DataFrame.

    Args:
        primary_df (pl.DataFrame): The DataFrame to which the aligned date column will be added.
        secondary_df (pl.DataFrame): The DataFrame providing candidate dates for alignment.
        primary_column (str): Column name in `primary_df` containing date values.
        secondary_column (str): Column name in `secondary_df` containing date values.

    Returns:
        pl.DataFrame: The original `primary_df` with an additional column containing
        aligned dates from `secondary_df`.
    """
    aligned_dates = align_import_dates(
        primary_df, secondary_df, primary_column, secondary_column
    )

    primary_df_with_aligned_dates = primary_df.join(
        aligned_dates, primary_column, "left"
    )

    return primary_df_with_aligned_dates


def align_import_dates(
    primary_df: pl.DataFrame,
    secondary_df: pl.DataFrame,
    primary_column: str,
    secondary_column: str,
) -> pl.DataFrame:
    """
    Generates a mapping of primary dates to the closest matching secondary dates.

    This function performs cross-joins over unique dates and determines the
    best non-future match between dates in the two DataFrames.

    Args:
        primary_df (pl.DataFrame): DataFrame containing the primary dates.
        secondary_df (pl.DataFrame): DataFrame containing candidate secondary dates.
        primary_column (str): Column name in `primary_df` with date values.
        secondary_column (str): Column name in `secondary_df` with date values.

    Returns:
        pl.DataFrame: A two-column DataFrame that maps each primary date
        to its aligned secondary date.
    """
    possible_matches = cross_join_unique_dates(
        primary_df, secondary_df, primary_column, secondary_column
    )

    aligned_dates = determine_best_date_matches(
        possible_matches, primary_column, secondary_column
    )

    return aligned_dates


def cross_join_unique_dates(
    primary_df: pl.DataFrame,
    secondary_df: pl.DataFrame,
    primary_column: str,
    secondary_column: str,
) -> pl.DataFrame:
    """
    Produces all possible date combinations between distinct primary and secondary dates.

    Args:
        primary_df (pl.DataFrame): DataFrame containing the primary date column.
        secondary_df (pl.DataFrame): DataFrame containing the secondary date column.
        primary_column (str): Name of the date column in `primary_df`.
        secondary_column (str): Name of the date column in `secondary_df`.

    Returns:
        pl.DataFrame: Cross-joined DataFrame of unique dates from both inputs.
        Contains two columns: `primary_column` and `secondary_column`.
    """
    primary_dates = primary_df.select(primary_column).unique()
    secondary_dates = secondary_df.select(secondary_column).unique()

    possible_matches = primary_dates.join(secondary_dates, how="cross")
    return possible_matches


def determine_best_date_matches(
    possible_matches: pl.DataFrame, primary_column: str, secondary_column: str
) -> pl.DataFrame:
    """
    Selects the nearest non-future secondary date for each primary date.

    This function:
      - Computes the day difference between primary and secondary dates.
      - Removes matches where the secondary date is in the future.
      - Selects the smallest non-negative difference for each primary date.

    Args:
        possible_matches (pl.DataFrame): Cross-joined DataFrame of date pairs.
        primary_column (str): Column name for primary dates.
        secondary_column (str): Column name for secondary dates.

    Returns:
        pl.DataFrame: Two-column DataFrame containing each primary date
        and its best-aligned secondary date.
    """
    date_diff: str = "date_diff"
    min_date_diff: str = "min_date_diff"
    possible_matches = possible_matches.with_columns(
        (pl.col(primary_column) - pl.col(secondary_column))
        .dt.total_days()
        .alias(date_diff)
    )

    possible_matches = possible_matches.filter(pl.col(date_diff) >= 0)

    possible_matches = possible_matches.with_columns(
        pl.min(date_diff).over(primary_column).alias(min_date_diff)
    )

    aligned_dates = possible_matches.filter(pl.col(min_date_diff) == pl.col(date_diff))

    return aligned_dates.select(primary_column, secondary_column)
