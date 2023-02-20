from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
)
import pyspark.sql.functions as F


def two_cols_are_equal_and_not_null(first_col: str, second_col: str):
    return (
        (F.col(first_col) == F.col(second_col))
        & F.col(second_col).isNotNull()
        & F.col(first_col).isNotNull()
    )


def calculate_jobcount_totalstaff_equal_wkrrecs(
    input_df, total_staff_column, worker_records_column, output_column_name
):
    return input_df.withColumn(
        output_column_name,
        F.when(
            (
                job_count_from_ascwds_is_not_populated(output_column_name)
                & two_cols_are_equal_and_not_null(
                    total_staff_column, worker_records_column
                )
            ),
            worker_records_column,
        ).otherwise(output_column_name),
    )
