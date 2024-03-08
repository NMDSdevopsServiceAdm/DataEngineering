import pyspark
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


def selected_ascwds_job_count_is_at_least_the_min_permitted(col_name: str):
    return F.col(col_name).isNotNull() & (
        F.col(col_name) >= calculation_constant.MIN_ASCWDS_FILLED_POSTS_PERMITTED
    )


def selected_ascwds_job_count_is_below_the_min_permitted(col_name: str):
    return F.col(col_name) < calculation_constant.MIN_ASCWDS_FILLED_POSTS_PERMITTED


def column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
    col_name: str, min_abs_diff: float
) -> bool:
    result = F.col(col_name) < min_abs_diff
    return result
