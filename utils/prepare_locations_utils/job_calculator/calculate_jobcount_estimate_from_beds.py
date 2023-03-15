import pyspark.sql
from pyspark.sql import functions as F

from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
    column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count,
)
from utils.prepare_locations_utils.job_calculator.calculation_constants import (
    JobCalculationConstants as job_calc_const,
)


def number_of_beds_in_location_exceeds_min_number_needed_for_calculation(
    col_name: str, threshold: int
) -> bool:
    return F.col(col_name) > threshold


def calculate_jobcount_estimate_based_on_bed_to_staff_calculation_formula():
    return job_calc_const.BEDS_TO_JOB_COUNT_INTERCEPT + (
        F.col("number_of_beds") * job_calc_const.BEDS_TO_JOB_COUNT_COEFFICIENT
    )


def caclulate_difference_columns_to_show_difference_between_bed_based_job_count_and_other_job_records(
    input_df,
):
    # Determine differences
    input_df = input_df.withColumn(
        "totalstaff_diff", F.abs(input_df.total_staff - input_df.bed_estimate_jobcount)
    )
    input_df = input_df.withColumn(
        "wkrrecs_diff",
        F.abs(input_df.worker_record_count - input_df.bed_estimate_jobcount),
    )
    input_df = input_df.withColumn(
        "totalstaff_percentage_diff",
        F.abs(input_df.totalstaff_diff / input_df.bed_estimate_jobcount),
    )
    input_df = input_df.withColumn(
        "wkrrecs_percentage_diff",
        F.abs(input_df.worker_record_count / input_df.bed_estimate_jobcount),
    )

    return input_df


def bed_estimated_job_count_is_populated(col_name: str) -> pyspark.sql.Column:
    return F.col(col_name).isNotNull()


def difference_between_total_staff_and_worker_record_is_less_than_abs_diff() -> bool:
    return (
        F.col("totalstaff_diff")
        < job_calc_const.MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def column_value_is_less_than_min_abs_pct_difference_between_total_staff_and_worker_record_count(
    col_name: str, min_abs_diff_pct: float
) -> bool:
    return F.col(col_name) < min_abs_diff_pct


def populate_job_count_based_on_worker_record_count():
    return F.col("worker_record_count")


def populate_job_count_with_total_staff_value():
    return F.col("total_staff")


def populate_job_count_column_with_job_count_data():
    return F.col("job_count_unfiltered")


def populate_job_count_with_average_of_total_staff_and_worker_record_count():
    return (F.col("total_staff") + F.col("worker_record_count")) / 2


def worker_recs_diff_or_worker_records_percentage_diff_within_tolerated_range():
    return (
        column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
            col_name="wkrrecs_diff",
            min_abs_diff=job_calc_const.MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    ) | (
        column_value_is_less_than_min_abs_pct_difference_between_total_staff_and_worker_record_count(
            col_name="wkrrecs_percentage_diff",
            min_abs_diff_pct=job_calc_const.MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    )


def total_staff_diff_or_total_staff_pct_diff_within_tolerated_range():
    return (
        column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
            col_name="totalstaff_diff",
            min_abs_diff=job_calc_const.MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    ) | (
        column_value_is_less_than_min_abs_pct_difference_between_total_staff_and_worker_record_count(
            col_name="totalstaff_percentage_diff",
            min_abs_diff_pct=job_calc_const.MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    )


def calculate_jobcount_estimate_from_beds(input_df):
    input_df = input_df.withColumn(
        "bed_estimate_jobcount",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count_unfiltered")
                & number_of_beds_in_location_exceeds_min_number_needed_for_calculation(
                    col_name="number_of_beds",
                    threshold=job_calc_const.BEDS_IN_WORKPLACE_THRESHOLD,
                )
            ),
            (calculate_jobcount_estimate_based_on_bed_to_staff_calculation_formula()),
        ).otherwise(None),
    )

    input_df = caclulate_difference_columns_to_show_difference_between_bed_based_job_count_and_other_job_records(
        input_df
    )
    # Bounding predictions to certain locations with differences in range
    # if total_staff and worker_record_count within 10% or < 5: return avg(total_staff + wkrrds)
    input_df = input_df.withColumn(
        "job_count_unfiltered",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count_unfiltered")
                & bed_estimated_job_count_is_populated("bed_estimate_jobcount")
                & (
                    total_staff_diff_or_total_staff_pct_diff_within_tolerated_range()
                    & worker_recs_diff_or_worker_records_percentage_diff_within_tolerated_range()
                )
            ),
            populate_job_count_with_average_of_total_staff_and_worker_record_count(),
        ).otherwise(populate_job_count_column_with_job_count_data()),
    )

    # if total_staff within 10% or < 5: return total_staff
    input_df = input_df.withColumn(
        "job_count_unfiltered",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count_unfiltered")
                & bed_estimated_job_count_is_populated("bed_estimate_jobcount")
                & (total_staff_diff_or_total_staff_pct_diff_within_tolerated_range())
            ),
            populate_job_count_with_total_staff_value(),
        ).otherwise(populate_job_count_column_with_job_count_data()),
    )

    # if worker_record_count within 10% or < 5: return worker_record_count
    input_df = input_df.withColumn(
        "job_count_unfiltered",
        F.when(
            (
                F.col("job_count_unfiltered").isNull()
                & F.col("bed_estimate_jobcount").isNotNull()
                & (
                    worker_recs_diff_or_worker_records_percentage_diff_within_tolerated_range()
                )
            ),
            populate_job_count_based_on_worker_record_count(),
        ).otherwise(populate_job_count_column_with_job_count_data()),
    )

    # Drop temporary columns
    columns_to_drop = [
        "bed_estimate_jobcount",
        "totalstaff_diff",
        "wkrrecs_diff",
        "totalstaff_percentage_diff",
        "wkrrecs_percentage_diff",
    ]

    input_df = input_df.drop(*columns_to_drop)

    return input_df
