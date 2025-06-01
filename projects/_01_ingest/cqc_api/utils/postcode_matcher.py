from pyspark.sql import DataFrame, functions as F
from typing import Tuple

import utils.cleaning_utils as cUtils
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)


def run_postcode_matching(
    locations_df: DataFrame,
    postcode_df: DataFrame,
) -> DataFrame:
    """
    Runs full postcode matching logic and raises error if final validation fails.

    This function consists of 5 steps:
        - Match postcodes where there is an exact match at that point in time.
        - (TO DO) If not, use the first exact matching postcode for that location ID where available.
        - (TO DO) If not, replace known postcode issues using the invalid postcode dictionary.
        - (TO DO) If not, match the postcode based on the first half of the postcode only (truncated postcode).
        - (TO DO) If not, raise an error to manually investigate any unmatched postcodes.

    Args:
        locations_df (DataFrame): DataFrame of workplaces with postcodes.
        postcode_df (DataFrame): ONS postcode directory.

    Returns:
        DataFrame: Fully matched DataFrame.
    """
    locations_df = clean_postcode_column(
        locations_df, CQCL.postal_code, CQCLClean.postcode_cleaned, drop_col=False
    )

    postcode_df = clean_postcode_column(
        postcode_df, ONSClean.postcode, CQCLClean.postcode_cleaned, drop_col=True
    )

    locations_df = cUtils.add_aligned_date_column(
        locations_df,
        postcode_df,
        CQCLClean.cqc_location_import_date,
        ONSClean.contemporary_ons_import_date,
    )

    original_matched_df, original_unmatched_df = join_postcode_data(
        locations_df, postcode_df, CQCLClean.postcode_cleaned
    )
    if original_unmatched_df.count() == 0:
        return original_matched_df

    # TODO remove this at the end, left in for investigating
    print(f"Unmatched postcode count: {original_unmatched_df.count()}")

    # TODO - use the first exact matching postcode for that location ID where available.

    # TODO - replace known postcode issues using the invalid postcode dictionary.

    # TODO - match the postcode based on the first half of the postcode only (truncated postcode).

    # TODO - raise an error to manually investigate any unmatched postcodes.


def clean_postcode_column(
    df: DataFrame, postcode_col: str, cleaned_col_name: str, drop_col: bool
) -> DataFrame:
    """
    Creates a clean version of the postcode column, in upper case with spaces removed.

    Args:
        df (DataFrame): CQC locations DataFrame with the postcode column in.
        postcode_col (str): The name of the postcode column.
        cleaned_col_name (str): The name of the cleaned postcode column.
        drop_col (bool): Drop the original column if True, otherwise keep the original column.

    Returns:
        DataFrame: A cleaned postcode column in upper case and blank spaces removed.
    """
    df = df.withColumn(
        cleaned_col_name,
        F.upper(F.regexp_replace(F.col(postcode_col), " ", "")),
    )
    if drop_col:
        df = df.drop(postcode_col)
    return df


def join_postcode_data(
    locations_df: DataFrame,
    postcode_df: DataFrame,
    postcode_col: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Joins postcode data into the locations DataFrame based on matching postcodes

    Args:
        locations_df (DataFrame): Workplace DataFrame with a postcode column.
        postcode_df (DataFrame): ONS Postcode directory DataFrame.
        postcode_col (str): Name of the postcode column to join on.

    Returns:
        Tuple[DataFrame, DataFrame]: Matched and unmatched DataFrames.
    """
    joined_df = locations_df.join(
        postcode_df,
        [ONSClean.contemporary_ons_import_date, postcode_col],
        "left",
    )
    matched_df = joined_df.filter(F.col(postcode_df.columns[-1]).isNotNull())

    unmatched_df = joined_df.filter(F.col(postcode_df.columns[-1]).isNull())
    unmatched_df = unmatched_df.select(*locations_df.columns)

    return matched_df, unmatched_df
