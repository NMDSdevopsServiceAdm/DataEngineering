import pyspark.sql.functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculation_constants import (
    ASCWDSFilledPostCalculationConstants as calculation_constant,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns


def ascwds_filled_posts_is_null():
    return F.col(IndCqcColumns.ascwds_filled_posts).isNull()


def selected_column_is_not_null(col_name: str):
    return F.col(col_name).isNotNull()


def selected_column_is_null(col_name: str):
    return F.col(col_name).isNull()


def selected_column_is_at_least_the_min_permitted_value(col_name: str):
    return F.col(col_name).isNotNull() & (
        F.col(col_name) >= calculation_constant.MIN_ASCWDS_FILLED_POSTS_PERMITTED
    )


def selected_column_is_below_the_min_permitted_value(col_name: str):
    return F.col(col_name) < calculation_constant.MIN_ASCWDS_FILLED_POSTS_PERMITTED


def column_value_is_less_than_max_absolute_difference(col_name: str) -> bool:
    return (
        F.col(col_name)
        < calculation_constant.MAX_ABSOLUTE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def mean_absolute_difference_less_than_max_pct_difference(
    abs_dff_col: str, comparison_col: str
):
    return (
        F.col(abs_dff_col) / F.col(comparison_col)
        < calculation_constant.MAX_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def two_cols_are_equal_and_at_least_minimum_permitted_value(
    first_col: str, second_col: str
):
    return (
        F.col(first_col) == F.col(second_col)
    ) & selected_column_is_at_least_the_min_permitted_value(first_col)
