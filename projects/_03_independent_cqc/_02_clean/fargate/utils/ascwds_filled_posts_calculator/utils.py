import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.calculation_constants import (
    ASCWDSFilledPostCalculationConstants as calculation_constant,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

absolute_difference: str = "absolute_difference"


def ascwds_filled_posts_is_null() -> pl.Expr:
    """
    Checks whether the ascwds_filled_posts column is null.

    This function returns a Polars expression that evaluates to True when the ascwds_filled_posts column contains a null value.
    It is intended to be used inside conditional expressions within LazyFrame transformations.

    Returns:
        pl.Expr: A Polars expression evaluating whether ascwds_filled_posts is null.
    """
    return pl.col(IndCQC.ascwds_filled_posts).is_null()


def selected_column_is_not_null(col_name: str) -> pl.Expr:
    """
    Checks whether the specified column contains non-null values.

    This function returns a Polars expression that evaluates to True when the given column
    contains a non-null value. It is intended to be used within conditional expressions inside
    LazyFrame transformations.

    Args:
        col_name (str): The name of the column to check for non-null values.

    Returns:
        pl.Expr: A Polars expression evaluating whether the specified column is not null.
    """
    return pl.col(col_name).is_not_null()


def selected_column_is_at_least_the_min_permitted_value(col_name: str) -> pl.Expr:
    """
    Checks whether the specified column contains a non-null value that is greater than
    or equal to the minimum permitted value for ASCWDS filled posts.

    This function builds a Polars expression that evaluates to True
    when:
    - the selected column is not null, and
    - the selected column value is greater than or equal to the configured minimum permitted threshold.

    Args:
        col_name (str): The name of the column to validate.

    Returns:
        pl.Expr: A Polars expression evaluating whether the column meets the minimum permitted value requirement.
    """
    return selected_column_is_not_null(col_name) & (
        pl.col(col_name) >= calculation_constant.MIN_ASCWDS_FILLED_POSTS_PERMITTED
    )


def absolute_difference_between_total_staff_and_worker_records_below_cut_off() -> (
    pl.Expr
):
    """
    Checks whether the absolute difference between the total staff and worker record bounded columns is below
    the configured cutoff.

    This function builds a Polars expression that evaluates to True
    when the absolute difference between:
        - IndCQC.total_staff_bounded
        - IndCQC.worker_records_bounded
    is less than the maximum permitted threshold defined in calculation constants.

    Returns:
        pl.Expr: A Polars expression evaluating whether the absolute difference between the two columns is below
                the cutoff.
    """
    return (
        absolute_difference_between_two_columns(
            IndCQC.total_staff_bounded,
            IndCQC.worker_records_bounded,
        )
        < calculation_constant.MAX_ABSOLUTE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def percentage_difference_between_total_staff_and_worker_records_below_cut_off() -> (
    pl.Expr
):
    """
    Checks whether the percentage difference between the total staff and worker record bounded columns is below
    the configured cutoff.

    This function builds a Polars expression that evaluates to True
    when the absolute difference between:
        - IndCQC.total_staff_bounded
        - IndCQC.worker_records_bounded
    divided by their average, is less than the maximum permitted percentage threshold defined in calculation constants.

    Returns:
        pl.Expr: A Polars expression evaluating whether the percentage difference between the two columns is below
                the cutoff.
    """
    return (
        (
            absolute_difference_between_two_columns(
                IndCQC.total_staff_bounded,
                IndCQC.worker_records_bounded,
            )
            / average_of_two_columns(
                IndCQC.total_staff_bounded,
                IndCQC.worker_records_bounded,
            )
        )
        < calculation_constant.MAX_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def two_cols_are_equal_and_at_least_minimum_permitted_value(
    first_col: str, second_col: str
) -> pl.Expr:
    """
    Checks whether two columns are equal and the first column meets the minimum permitted value requirement.

    This function builds a Polars expression that evaluates to True when:
        - the values in `first_col` and `second_col` are equal, and
        - `first_col` is at least the minimum permitted value
          (as determined by `selected_column_is_at_least_the_min_permitted_value`).

    Args:
        first_col (str): The name of the first column to compare.
        second_col (str): The name of the second column to compare.

    Returns:
        pl.Expr: A Polars expression evaluating whether the columns
                 are equal and the first column meets the minimum threshold.
    """
    return (
        pl.col(first_col) == pl.col(second_col)
    ) & selected_column_is_at_least_the_min_permitted_value(first_col)


def absolute_difference_between_two_columns(first_col: str, second_col: str) -> pl.Expr:
    """
    Calculates the absolute difference between two columns.

    This function builds a Polars expression that computes the absolute difference between the values in `first_col`
    and `second_col` for each row.

    Args:
        first_col (str): The name of the first column.
        second_col (str): The name of the second column.

    Returns:
        pl.Expr: A Polars expression evaluating the absolute difference
                 between the two columns.
    """
    return (pl.col(first_col) - pl.col(second_col)).abs()


def average_of_two_columns(first_col: str, second_col: str) -> pl.Expr:
    """
    Calculates the average of two columns.

    This function builds a Polars expression that computes the average of the values in `first_col` and `second_col`
    for each row.

    Args:
        first_col (str): The name of the first column.
        second_col (str): The name of the second column.

    Returns:
        pl.Expr: A Polars expression evaluating the row-wise average
                 of the two columns.
    """
    return (pl.col(first_col) + pl.col(second_col)) / 2


def add_source_description_to_source_column(
    lf: pl.LazyFrame,
    populated_column_name: str,
    source_column_name: str,
    source_description: str,
) -> pl.LazyFrame:
    """
    Adds a source description to a specified column if the populated column is not null and the source column is null.

    This function is run following a calculation that populates a column with data.
    If the populated column has a value and the source column is null, it updates the source column with the provided source description.
    This is useful for tracking the origin of the data in the source column.

    Args:
        lf (pl.LazyFrame): The input polars LazyFrame.
        populated_column_name (str): Name of the column used to determine if the row should have a source description.
        source_column_name (str): Name of the column to update with the source description.
        source_description (str): The description to insert into the source column.

    Returns:
        pl.LazyFrame: A new polars LazyFrame with the source column updated.
    """
    return lf.with_columns(
        pl.when(
            (pl.col(populated_column_name).is_not_null())
            & (pl.col(source_column_name).is_null())
        )
        .then(pl.lit(source_description))
        .otherwise(pl.col(source_column_name))
        .alias(source_column_name)
    )
