import pyspark
import pyspark.sql.functions as F

from utils.prepare_locations_utils.job_calculator.calculation_constants import (
    JobCalculationConstants,
)


def job_count_from_ascwds_is_not_populated(col_name: str) -> pyspark.sql.Column:
    result = F.col(col_name).isNull()
    return result


def selected_column_is_not_null(col_name: str):
    return F.col(col_name).isNotNull()


def selected_column_is_null(col_name: str):
    return F.col(col_name).isNull()


def selected_ascwds_job_count_is_at_least_the_min_permitted(col_name: str):
    return F.col(col_name) >= JobCalculationConstants().MIN_ASCWDS_JOB_COUNT_PERMITTED


def column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
    col_name: str, min_abs_diff: float
) -> bool:
    result = F.col(col_name) < min_abs_diff
    return result
