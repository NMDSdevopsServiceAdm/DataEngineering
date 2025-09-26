import polars as pl


def add_aligned_date_column(
    primary_df: pl.DataFrame,
    secondary_df: pl.DataFrame,
    primary_column: str,
    secondary_column: str,
) -> pl.DataFrame:
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
    primary_dates = primary_df.select(primary_column).unique()
    secondary_dates = secondary_df.select(secondary_column).unique()

    possible_matches = primary_dates.join(secondary_dates, how="cross")
    return possible_matches


def determine_best_date_matches(
    possible_matches: pl.DataFrame, primary_column: str, secondary_column: str
) -> pl.DataFrame:
    date_diff: str = "date_diff"
    min_date_diff: str = "min_date_diff"
    possible_matches = possible_matches.with_columns(
        (pl.col(primary_column) - pl.col(secondary_column))
        .dt.total_days()
        .alias(date_diff)
    )

    possible_matches = possible_matches.filter(possible_matches[date_diff] >= 0)

    possible_matches = possible_matches.with_columns(
        pl.min(date_diff).over(primary_column).alias(min_date_diff)
    )

    aligned_dates = possible_matches.filter(
        possible_matches[min_date_diff] == possible_matches[date_diff]
    )

    return aligned_dates.select(primary_column, secondary_column)
