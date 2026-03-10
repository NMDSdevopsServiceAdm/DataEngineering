import polars as pl
from typing import List

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome


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
            (pl.col(IndCQC.care_home) == CareHome.care_home)
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
        lf (Lazyframe): A lazyframe containing the partition keys year, month and day.

    Returns:
        Lazyframe: A lazyframe with only the first import date of each month.
    """
    earliest_day_in_month = "first_day_in_month"
    lf = lf.with_columns(
        pl.col(Keys.day)
        .first()
        .over([Keys.year, Keys.month], order_by=Keys.day)
        .alias(earliest_day_in_month)
    )
    return lf.filter(pl.col(earliest_day_in_month) == pl.col(Keys.day)).drop(
        earliest_day_in_month
    )


def create_banded_bed_count_column(
    input_lf: pl.LazyFrame, new_col: str, splits: List[float]
) -> pl.LazyFrame:
    """
    Creates a new column in the input Lazyframe that categorises the number of beds into defined bands.

    This function uses a Bucketizer to categorise the number of beds into specified bands.
    The banded bed counts are joined into the original Lazyframe.

    If the location is non-res then zero is returned.

    Args:
        input_df (Lazyframe): The Lazyframe containing the column 'number_of_beds' to be banded.
        new_col (str): The name of the output column with the banded values.
        splits (List[float]): The list of split points for bucketing (must be strictly increasing).

    Returns:
        Lazyframe: A new Lazyframe that includes the original data along with a new column 'number_of_beds_banded'.
    """
    zero: float = 0.0

    number_of_beds_lf = (
        input_lf.select(IndCQC.number_of_beds)
        .filter(pl.col(IndCQC.number_of_beds).is_not_null())
        .unique()
    )

    number_of_beds_with_bands_lf = number_of_beds_lf.with_columns(
        pl.col(IndCQC.number_of_beds).cut(splits).alias(new_col)
    )
    output_lf = input_lf.join(
        number_of_beds_with_bands_lf, IndCQC.number_of_beds, "left"
    )

    return output_lf.with_columns(
        new_col,
        pl.when(pl.col(IndCQC.care_home) == CareHome.not_care_home)
        .then(zero)
        .otherwise(pl.col(new_col)),
    )
