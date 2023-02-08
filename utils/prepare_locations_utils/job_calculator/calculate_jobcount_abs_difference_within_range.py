from pyspark.sql import functions as F

from utils.prepare_locations_utils.job_calculator.calculation_constants import (
    JobCalculationConstants,
)
from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count,
)


def calculate_jobcount_abs_difference_within_range(input_df):
    # Abs difference between total_staff & worker_record_count < 5 or < 10% take average:
    input_df = input_df.withColumn(
        "abs_difference", F.abs(input_df.total_staff - input_df.worker_record_count)
    )

    input_df = input_df.withColumn(
        "job_count",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count")
                & (
                    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
                        col_name="abs_difference",
                        min_abs_diff=JobCalculationConstants().MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
                    )
                    | mean_abs_difference_less_than_min_pct_difference(
                        abs_dff_col="abs_difference",
                        comparison_col="total_staff",
                        min_diff_val=JobCalculationConstants().MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
                    )
                )
            ),
            (F.col("total_staff") + F.col("worker_record_count")) / 2,
        ).otherwise(F.col("job_count")),
    )

    input_df = input_df.drop("abs_difference")

    return input_df


def mean_abs_difference_less_than_min_pct_difference(
    abs_dff_col: str, comparison_col: str, min_diff_val: float
):
    return F.col(abs_dff_col) / F.col(comparison_col) < min_diff_val
