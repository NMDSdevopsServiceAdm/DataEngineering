from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
)
from utils.prepare_locations_utils.job_calculator.calculation_constants import (
    JobCalculationConstants,
)
import pyspark.sql.functions as F


def two_cols_are_equal_and_at_least_minimum_job_count_permitted(
    first_col: str, second_col: str
):
    return (
        (F.col(first_col) == F.col(second_col))
        & F.col(first_col).isNotNull()
        & (F.col(first_col) >= JobCalculationConstants().MIN_ASCWDS_JOB_COUNT_PERMITTED)
    )


def calculate_jobcount_totalstaff_equal_wkrrecs(
    input_df, total_staff_column: str, worker_records_column: str, output_column_name
):
    return input_df.withColumn(
        output_column_name,
        F.when(
            (
                job_count_from_ascwds_is_not_populated(output_column_name)
                & two_cols_are_equal_and_at_least_minimum_job_count_permitted(
                    total_staff_column, worker_records_column
                )
            ),
            F.col(worker_records_column),
        ).otherwise(F.col(output_column_name)),
    )
