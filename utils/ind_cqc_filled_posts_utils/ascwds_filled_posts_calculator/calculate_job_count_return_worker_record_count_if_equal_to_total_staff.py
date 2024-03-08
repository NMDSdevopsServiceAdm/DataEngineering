import pyspark.sql.functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_ascwds_job_count_is_at_least_the_min_permitted,
)


def two_cols_are_equal_and_at_least_minimum_job_count_permitted(
    first_col: str, second_col: str
):
    return (
        F.col(first_col) == F.col(second_col)
    ) & selected_ascwds_job_count_is_at_least_the_min_permitted(first_col)


def calculate_jobcount_totalstaff_equal_wkrrecs(
    input_df, total_staff_column: str, worker_records_column: str, output_column_name
):
    return input_df.withColumn(
        output_column_name,
        F.when(
            (
                ascwds_filled_posts_is_null()
                & two_cols_are_equal_and_at_least_minimum_job_count_permitted(
                    total_staff_column, worker_records_column
                )
            ),
            F.col(worker_records_column),
        ).otherwise(F.col(output_column_name)),
    )
