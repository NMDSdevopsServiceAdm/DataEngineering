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
    input_df_with_job_count_temp = input_df.withColumn("job_temp", F.lit(None))

    input_df_with_job_count_temp = input_df_with_job_count_temp.withColumn(
        "job_temp",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count")
                & total_staff_or_worker_record_count_less_than_permitted_minimum()
            ),
            select_the_bigger_value_between_total_staff_and_worker_rec_count(),
        ),
    )

    input_df_with_job_source = input_df_with_job_count_temp.withColumn(
        "job_count_source", F.when((F.col("job_temp").isNotNull()), "Tiny Values")
    )

    input_df_with_job_count_pop = input_df_with_job_source.withColumn(
        "job_count",
        F.when((F.col("job_temp").isNotNull()), F.col("job_temp")).otherwise(
            F.col("job_count")
        ),
    )

    output_df = input_df_with_job_count_pop.drop("job_temp")

    return output_df
