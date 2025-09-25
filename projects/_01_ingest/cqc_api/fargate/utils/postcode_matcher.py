from typing import Tuple

import polars as pl

from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)


def clean_postcode_column(
    df: pl.DataFrame, postcode_col: str, cleaned_col_name: str, drop_col: bool
) -> pl.DataFrame:
    """
    Creates a clean version of the postcode column, in upper case with spaces removed.

    Args:
        df (pl.DataFrame): CQC locations DataFrame with the postcode column in.
        postcode_col (str): The name of the postcode column.
        cleaned_col_name (str): The name of the cleaned postcode column.
        drop_col (bool): Drop the original column if True, otherwise keep the original column.

    Returns:
        pl.DataFrame: A cleaned postcode column in upper case and blank spaces removed.
    """
    df = df.with_columns(
        pl.col(postcode_col)
        .str.replace_all(" ", "")
        .str.to_uppercase()
        .alias(cleaned_col_name)
    )

    if drop_col:
        df = df.drop(postcode_col)
    return df


def join_postcode_data(
    locations_df: pl.DataFrame,
    postcode_df: pl.DataFrame,
    postcode_col: str,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Joins postcode data into the locations DataFrame based on matching postcodes.

    A successful join is determined by the presence of a non-null value in the
    current_cssr column of the ONS postcode directory.

    Args:
        locations_df (pl.DataFrame): Workplace DataFrame with a postcode column.
        postcode_df (pl.DataFrame): ONS Postcode directory DataFrame.
        postcode_col (str): Name of the postcode column to join on.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: Matched and unmatched DataFrames.
    """
    joined_df = locations_df.join(
        postcode_df,
        [ONSClean.contemporary_ons_import_date, postcode_col],
        "left",
    )
    matched_df = joined_df.filter(pl.col(ONSClean.current_cssr).is_not_null())

    unmatched_df = joined_df.filter(pl.col(ONSClean.current_cssr).is_null())
    unmatched_df = unmatched_df.select(*locations_df.columns)

    return matched_df, unmatched_df
