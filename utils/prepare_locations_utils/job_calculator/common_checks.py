import pyspark
import pyspark.sql.functions as F


def job_count_from_ascwds_is_not_populated(col_name: str) -> pyspark.sql.Column:
    return F.col(col_name).isNull()


def selected_column_is_not_null(col_name: str):
    return F.col(col_name).isNotNull()


def selected_column_is_null(col_name: str):
    return F.col(col_name).isNull()


def column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
        col_name: str, min_abs_diff: float
) -> bool:
    return F.col(col_name) < min_abs_diff
