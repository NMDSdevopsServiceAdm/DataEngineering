from typing import Generator, List

import polars as pl

from polars_utils.expressions import is_care_home, is_not_care_home
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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


def apply_categorical_labels(
    lf: pl.LazyFrame,
    labels_lf: pl.LazyFrame,
    column_names: list,
    add_as_new_column: bool = True,
    reverse_mapping: bool = False,
) -> pl.LazyFrame:
    """
    Apply categorical label mappings to one or more columns.

    For each column in `column_names`, the corresponding codes and labels in the labels LazyFrame are joined as a new column in the original LazyFrame. The column is then name appropriately.dictionary in `labels`
    is converted to a LazyFrame and joined to `lf` to map codes to labels.
    Partial mappings are supported: unmapped values will be preserved.

    Labels can either be added as new columns or replace the original columns.

    reverse_mapping is an optional argument to revert columns from a label string to a code string.
    Warning: the values in 'labels' dict should be unique, otherwise only first key of
    duplicate values will be used in the mapping.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.
        labels_lf (pl.LazyFrame): LazyFrame with mapping dictionaries for columns.
        column_names (list): List of column names to apply label mappings to.
        add_as_new_column (bool, optional): If True, adds a new column with
            "_labels" suffix. If False, replaces the original column.
            Defaults to True.
        reverse_mapping (bool, optional): If True, reverts labels to codes.

    Returns:
        pl.LazyFrame: LazyFrame with categorical labels applied. Unmapped values
            are preserved.

    Raises:
        ValueError: If column to apply labels to is not in LazyFrame.
        KeyError: If column has no mapping in labels dict.

    """
    column_name_col = "column_name"

    for column_name in column_names:
        if column_name not in lf.collect_schema().names():
            raise ValueError(f"Column {column_name} not found in LazyFrame.")
        if (
            labels_lf.filter(pl.col(column_name_col) == column_name).collect().height
            == 0
        ):
            raise KeyError(f"No label mapping found for {column_name}.")

    lf = lf.with_columns(
        labels_generator(labels_lf, column_names, add_as_new_column, reverse_mapping)
    )
    return lf


def labels_generator(
    labels_lf: pl.LazyFrame,
    column_names: list,
    add_as_new_column: bool,  # TODO
    reverse_mapping: bool,
) -> Generator[pl.Expr, None, None]:
    column_name_col = "column_name"
    if reverse_mapping == False:
        join_col = "code"
        new_col = "label"
        suffix = "_labels"
    else:
        join_col = "label"
        new_col = "code"
        suffix = "_codes"
    for column_name in column_names:
        old_vals = (
            labels_lf.filter(pl.col(column_name_col) == column_name)
            .sort(join_col, new_col)
            .unique(subset=[column_name_col, join_col], keep="first")
            .collect()
            .sort(join_col, new_col)
            .get_column(join_col)
        )
        new_vals = (
            labels_lf.filter(pl.col(column_name_col) == column_name)
            .sort(join_col, new_col)
            .unique(subset=[column_name_col, join_col], keep="first")
            .collect()
            .sort(join_col, new_col)
            .get_column(new_col)
        )
        if add_as_new_column == True:
            yield (
                pl.col(column_name)
                .replace_strict(
                    old=old_vals,
                    new=new_vals,
                    default=pl.col(column_name),
                )
                .name.suffix(suffix)
            )
        elif add_as_new_column == False:
            yield (
                pl.col(column_name).replace_strict(
                    old=old_vals,
                    new=new_vals,
                    default=pl.col(column_name),
                )
            )


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


def calculate_filled_posts_per_bed_ratio(
    input_lf: pl.LazyFrame, filled_posts_column: str, new_column_name: str
) -> pl.LazyFrame:
    """
    Add a column with the filled post per bed ratio for care homes.

    Args:
        input_lf (pl.LazyFrame): A LazyFrame containing the given column, care_home
            and numberofbeds.
        filled_posts_column (str): The name of the column to use for calculating
            the ratio.
        new_column_name (str): The name to give the new column.

    Returns:
        pl.LazyFrame: The same LazyFrame with an additional column contianing the
            filled posts per bed ratio for care homes.
    """
    lf = input_lf.with_columns(
        pl.when(
            is_care_home()
            & pl.col(IndCQC.number_of_beds).is_not_null()
            & (pl.col(IndCQC.number_of_beds) != 0)
        )
        .then(pl.col(filled_posts_column) / pl.col(IndCQC.number_of_beds))
        .otherwise(pl.lit(None))
        .alias(new_column_name)
    )

    return lf


def reduce_dataset_to_earliest_file_per_month(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Reduce the dataset to the first file of every month.

    This function identifies the date of the first import date in each month and then filters the dataset to those import dates only.

    Args:
        lf (pl.LazyFrame): A lazyframe containing the partition keys year, month and day.

    Returns:
        pl.LazyFrame: A lazyframe with only the first import date of each month.
    """
    date_col = pl.col(IndCQC.cqc_location_import_date)

    expr = date_col.min().over(date_col.dt.year(), date_col.dt.month())

    return lf.filter(date_col == expr)


def create_banded_bed_count_column(
    input_lf: pl.LazyFrame, new_col: str, splits: List[float]
) -> pl.LazyFrame:
    """
    Creates a new column in the input Lazyframe that categorises the number of beds into defined bands.

    This function uses a Bucketizer to categorise the number of beds into specified bands.
    The banded bed counts are joined into the original Lazyframe.

    If the location is non-res then zero is returned.

    Args:
        input_lf (pl.LazyFrame): The Lazyframe containing the column 'number_of_beds' to be banded.
        new_col (str): The name of the output column with the banded values.
        splits (List[float]): The list of split points for bucketing (must be strictly increasing).

    Returns:
        pl.LazyFrame: A new Lazyframe that includes the original data along with a new column 'number_of_beds_banded'.
    """
    zero: float = 0.0

    labels = [str(i) for i in range(len(splits[1:-1]) + 1)]
    expr = (
        pl.col(IndCQC.number_of_beds)
        .cut(breaks=splits[1:-1], labels=labels, left_closed=True)
        .cast(pl.String)
        .cast(pl.Float64)
    )

    return input_lf.with_columns(
        pl.when(is_not_care_home()).then(zero).otherwise(expr).alias(new_col)
    )
