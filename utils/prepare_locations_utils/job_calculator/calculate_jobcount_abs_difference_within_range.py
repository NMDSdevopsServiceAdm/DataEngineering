from pyspark.sql import functions as F

from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count,
    selected_ascwds_job_count_is_at_least_the_min_permitted,
)
from utils.prepare_locations_utils.job_calculator.calculation_constants import (
    JobCalculationConstants,
)


def calculate_jobcount_abs_difference_within_range(
    input_df, total_staff_column: str, worker_records_column: str, output_column_name
):
    # Abs difference between total_staff & worker_record_count < 5 or < 10% take average:
    input_df = input_df.withColumn(
        "abs_difference",
        F.abs(F.col(total_staff_column) - F.col(worker_records_column)),
    )

    input_df = input_df.withColumn(
        output_column_name,
        F.when(
            (
                job_count_from_ascwds_is_not_populated(output_column_name)
                & selected_ascwds_job_count_is_at_least_the_min_permitted(
                    total_staff_column
                )
                & selected_ascwds_job_count_is_at_least_the_min_permitted(
                    worker_records_column
                )
                & (
                    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
                        col_name="abs_difference",
                        min_abs_diff=JobCalculationConstants().MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
                    )
                    | mean_abs_difference_less_than_min_pct_difference(
                        abs_dff_col="abs_difference",
                        comparison_col=total_staff_column,
                        min_diff_val=JobCalculationConstants().MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
                    )
                )
            ),
            (F.col(total_staff_column) + F.col(worker_records_column)) / 2,
        ).otherwise(F.col(output_column_name)),
    )

    input_df = input_df.drop("abs_difference")

    return input_df


def mean_abs_difference_less_than_min_pct_difference(
    abs_dff_col: str, comparison_col: str, min_diff_val: float
):
    return F.col(abs_dff_col) / F.col(comparison_col) < min_diff_val
