import pyspark.sql.functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculation_constants import (
    ASCWDSFilledPostCalculationConstants as calculation_constant,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

absolute_difference: str = "absolute_difference"


def ascwds_filled_posts_is_null() -> bool:
    return F.col(IndCQC.ascwds_filled_posts).isNull()


def selected_column_is_not_null(col_name: str) -> bool:
    return F.col(col_name).isNotNull()


def selected_column_is_null(col_name: str) -> bool:
    return F.col(col_name).isNull()


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
