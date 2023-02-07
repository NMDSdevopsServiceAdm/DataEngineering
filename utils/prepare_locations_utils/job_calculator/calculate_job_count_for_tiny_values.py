from pyspark.sql import functions as F

from utils.prepare_locations_utils.job_calculator.calculation_constants import (
    JobCalculationConstants,
)
from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
)


def total_staff_or_worker_record_count_less_than_permitted_minimum():
    return (
        F.col("total_staff") < JobCalculationConstants().MIN_TOTAL_STAFF_VALUE_PERMITTED
    ) | (
        F.col("worker_record_count")
        < JobCalculationConstants().MIN_WORKER_RECORD_COUNT_PERMITTED
    )


def select_the_bigger_value_between_total_staff_and_worker_rec_count():
    return F.greatest(F.col("total_staff"), F.col("worker_record_count"))


def calculate_jobcount_handle_tiny_values(input_df):
    # total_staff or worker_record_count < 3: return max
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count")
                & total_staff_or_worker_record_count_less_than_permitted_minimum()
            ),
            select_the_bigger_value_between_total_staff_and_worker_rec_count(),
        ).otherwise(F.col("job_count")),
    )
