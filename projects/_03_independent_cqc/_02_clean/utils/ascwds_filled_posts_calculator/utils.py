from pyspark.sql import DataFrame, functions as F

from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.calculation_constants import (
    ASCWDSFilledPostCalculationConstants as calculation_constant,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

absolute_difference: str = "absolute_difference"


def ascwds_filled_posts_is_null() -> bool:
    return F.col(IndCQC.ascwds_filled_posts).isNull()


def selected_column_is_not_null(col_name: str) -> bool:
    return F.col(col_name).isNotNull()


def selected_column_is_at_least_the_min_permitted_value(col_name: str) -> bool:
    return selected_column_is_not_null(col_name) & (
        F.col(col_name) >= calculation_constant.MIN_ASCWDS_FILLED_POSTS_PERMITTED
    )


def absolute_difference_between_total_staff_and_worker_records_below_cut_off() -> bool:
    return (
        absolute_difference_between_two_columns(
            IndCQC.total_staff_bounded, IndCQC.worker_records_bounded
        )
        < calculation_constant.MAX_ABSOLUTE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def percentage_difference_between_total_staff_and_worker_records_below_cut_off() -> (
    bool
):
    return (
        (
            absolute_difference_between_two_columns(
                IndCQC.total_staff_bounded, IndCQC.worker_records_bounded
            )
            / average_of_two_columns(
                IndCQC.total_staff_bounded, IndCQC.worker_records_bounded
            )
        )
        < calculation_constant.MAX_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def two_cols_are_equal_and_at_least_minimum_permitted_value(
    first_col: str, second_col: str
) -> bool:
    return (
        F.col(first_col) == F.col(second_col)
    ) & selected_column_is_at_least_the_min_permitted_value(first_col)


def absolute_difference_between_two_columns(first_col: str, second_col: str) -> int:
    return F.abs(F.col(first_col) - F.col(second_col))


def average_of_two_columns(first_col: str, second_col: str):
    return (F.col(first_col) + F.col(second_col)) / 2


def add_source_description_to_source_column(
    input_df: DataFrame,
    populated_column_name: str,
    source_column_name: str,
    source_description: str,
) -> DataFrame:
    """
    Adds a source description to a specified column if the populated column is not null and the source column is null.

    This function is run following a calculation that populates a column with data.
    If the populated column has a value and the source column is null, it updates the source column with the provided source description.
    This is useful for tracking the origin of the data in the source column.

    Args:
        input_df (DataFrame): The input Spark DataFrame.
        populated_column_name (str): Name of the column used to determine if the row should have a source description.
        source_column_name (str): Name of the column to update with the source description.
        source_description (str): The description to insert into the source column.

    Returns:
        DataFrame: A new DataFrame with the source column updated.
    """
    return input_df.withColumn(
        source_column_name,
        F.when(
            (
                F.col(populated_column_name).isNotNull()
                & F.col(source_column_name).isNull()
            ),
            source_description,
        ).otherwise(F.col(source_column_name)),
    )
