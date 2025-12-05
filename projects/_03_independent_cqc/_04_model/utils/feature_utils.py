from typing import Dict

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_array_column_count(
    lf: pl.LazyFrame, new_col_name: str, col_to_check: str
) -> pl.LazyFrame:
    """
    Add a new column with the count of items in an array column.

    This function adds a new column to the given data frame which contains the count of items in the specified array column.
    If the array column is empty, the count will return 0.

    Args:
        lf (pl.LazyFrame): A LazyFrame with an array column.
        new_col_name (str): A name for the new column with the count of items.
        col_to_check (str): The name of the array column.

    Returns:
        pl.LazyFrame: A LazyFrame with an extra column with the count of items in the specified array.
    """
    return lf.with_columns(
        pl.col(col_to_check).list.len().fill_null(0).cast(pl.UInt32).alias(new_col_name)
    )


def add_date_index_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds an index column ordered by `cqc_location_import_date` and grouped by `care_home`.

    A "dense" rank is used as it assigns the same index to identical dates but does not leave gaps in the sequence.
    For example, if three rows share the same date, they all receive the same index value, and the next distinct
    date receives the next integer, such as 1, 1, 1, 2.
    This differs from standard ranking, which would assign ranks 1, 1, 1, 4 for the same scenario.

    Args:
        lf (pl.LazyFrame): Input DataFrame.

    Returns:
        pl.LazyFrame: LazyFrame with an added index column.
    """
    return lf.with_columns(
        pl.col(IndCQC.cqc_location_import_date)
        .rank(method="dense")
        .over(IndCQC.care_home)
        .alias(IndCQC.cqc_location_import_date_indexed)
    )


def cap_integer_at_max_value(
    lf: pl.LazyFrame, col_name: str, max_value: pl.Int32, new_col_name: str
) -> pl.LazyFrame:
    """
    Caps the non-null values in a specified column at a given maximum value and stores the result in a new column.

    Null values remain as null.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.
        col_name (str): The name of the column to be capped.
        max_value (pl.Int32): The maximum value allowed for the column.
        new_col_name (str): The name of the new column to store the capped values.

    Returns:
        pl.LazyFrame: A new LazyFrame with the capped values stored in the new column, preserving null values.
    """
    return lf.with_columns(
        pl.when(pl.col(col_name).is_not_null())
        .then(pl.min_horizontal(pl.col(col_name), pl.lit(max_value)))
        .otherwise(None)
        .alias(new_col_name)
    )


def expand_encode_and_extract_features(
    lf: pl.LazyFrame, col_name: str, lookup_dict: Dict[str, str], is_array_col: bool
) -> pl.LazyFrame:
    """
    Expands a categorical or array column and converts values into binary variables.

    This function iterates through a lookup dictionary and creates new binary columns
    where each column corresponds to a key in the dictionary.
    If the column is an array, it checks for array membership using 'array_contains'.
    Otherwise, it performs an equality comparison.

    Args:
        lf (pl.LazyFrame): Input LazyFrame with column to convert.
        col_name (str): Name of the column to be expanded and encoded.
        lookup_dict (Dict[str, str]): Dictionary where keys are new column names and
            values are the lookup values to compare against.
        is_array_col (bool): If True, treats the column as an array and checks for
            array membership. If False, performs equality comparison.

    Returns:
        pl.LazyFrame: DataFrame with new binary columns added.
    """
    for key, value in lookup_dict.items():
        if is_array_col:
            lf = lf.with_columns(
                pl.col(col_name).list.contains(value).cast(pl.Int8).alias(key)
            )
        else:
            lf = lf.with_columns((pl.col(col_name) == value).cast(pl.Int8).alias(key))

    return lf


def group_rural_urban_sparse_categories(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a new rural-urban indicator column and recodes any category containing the word "sparse".

    All values that include "sparse" (case-insensitive) are replaced with "Sparse setting".
    All other values are copied unchanged.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the rural urban indicator column.

    Returns:
        pl.LazyFrame: LazyFrame with the new rural urban indicator column with recoded sparse categories.
    """
    return lf.with_columns(
        pl.when(
            pl.col(IndCQC.current_rural_urban_indicator_2011)
            .str.to_lowercase()
            .str.contains("sparse")
        )
        .then(pl.lit("Sparse setting"))
        .otherwise(pl.col(IndCQC.current_rural_urban_indicator_2011))
        .alias(IndCQC.current_rural_urban_indicator_2011_for_non_res_model)
    )


def select_and_filter_features_data(
    lf: pl.LazyFrame,
    features_list: list[str],
    dependent_col: str,
    partition_keys: list[str],
) -> pl.DataFrame:
    """
    Selects columns from a Polars LazyFrame and filters to non-null feature columns.

    This function:
        - Checks if all columns we want to select exist in the DataFrame.
        - If it does, it selects those columns. If not, it raises an error.
        - Filters the DataFrame to keep only rows where all feature columns are non-null.
        - Casts partition columns to string type (required for sinking to parquet).

    Args:
        lf (pl.LazyFrame): Input Polars LazyFrame.
        features_list (list[str]): List of feature column names.
        dependent_col (str): The name of the dependent column.
        partition_keys (list[str]): List of column names used for partitioning.

    Returns:
        pl.DataFrame: Polars DataFrame containing only selected columns and rows
                      where all feature columns are non-null.

    Raises:
        ValueError: If any required columns are missing from the DataFrame.
    """
    select_cols = (
        [
            IndCQC.location_id,
            IndCQC.cqc_location_import_date,
            dependent_col,
        ]
        + features_list
        + partition_keys
    )

    missing_cols = [
        col for col in select_cols if col not in lf.collect_schema().names()
    ]
    if missing_cols:
        raise ValueError(f"Missing columns in LazyFrame: {missing_cols}")

    lf = lf.select(select_cols).filter(
        pl.all_horizontal([pl.col(feature).is_not_null() for feature in features_list])
    )

    return lf.with_columns([pl.col(c).cast(pl.Utf8) for c in partition_keys])
