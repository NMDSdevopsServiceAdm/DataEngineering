import pyspark.sql
from pyspark.sql import functions as F

from utils.prepare_locations_utils.job_calculator.common_checks import job_count_from_ascwds_is_not_populated

BEDS_IN_WORKPLACE_THRESHOLD = 0

BEDS_TO_JOB_COUNT_INTERCEPT = 8.40975704621392
BEDS_TO_JOB_COUNT_COEFFICIENT = 1.0010753137758377001
MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT = 5
MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT = 0.1

MIN_TOTAL_STAFF_VALUE_PERMITTED = 3
MIN_WORKER_RECORD_COUNT_PERMITTED = 3





def number_of_beds_in_location_exceeds_min_number_needed_for_calculation(
        col_name: str, threshold: int
) -> bool:
    return F.col(col_name) > threshold


def calculate_jobcount_estimate_based_on_bed_to_staff_calculation_formula():
    return BEDS_TO_JOB_COUNT_INTERCEPT + (
            F.col("number_of_beds") * BEDS_TO_JOB_COUNT_COEFFICIENT
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
            < MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
    )


def column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
        col_name: str, min_abs_diff: float
) -> bool:
    return F.col(col_name) < min_abs_diff


def column_value_is_less_than_min_abs_pct_difference_between_total_staff_and_worker_record_count(
        col_name: str, min_abs_diff_pct: float
) -> bool:
    return F.col(col_name) < min_abs_diff_pct


def calculate_jobcount_estimate_from_beds(input_df):
    input_df = input_df.withColumn(
        "bed_estimate_jobcount",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated("job_count")
                    & number_of_beds_in_location_exceeds_min_number_needed_for_calculation(
                col_name="number_of_beds", threshold=BEDS_IN_WORKPLACE_THRESHOLD
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
        "job_count",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated("job_count")
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
        "job_count",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated("job_count")
                    & bed_estimated_job_count_is_populated("bed_estimate_jobcount")
                    & (total_staff_diff_or_total_staff_pct_diff_within_tolerated_range())
            ),
            populate_job_count_with_total_staff_value(),
        ).otherwise(populate_job_count_column_with_job_count_data()),
    )

    # if worker_record_count within 10% or < 5: return worker_record_count
    input_df = input_df.withColumn(
        "job_count",
        F.when(
            (
                    F.col("job_count").isNull()
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


def populate_job_count_based_on_worker_record_count():
    return F.col("worker_record_count")


def populate_job_count_with_total_staff_value():
    return F.col("total_staff")


def populate_job_count_column_with_job_count_data():
    return F.col("job_count")


def populate_job_count_with_average_of_total_staff_and_worker_record_count():
    return (F.col("total_staff") + F.col("worker_record_count")) / 2


def worker_recs_diff_or_worker_records_percentage_diff_within_tolerated_range():
    return (
        column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
            col_name="wkrrecs_diff",
            min_abs_diff=MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    ) | (
        column_value_is_less_than_min_abs_pct_difference_between_total_staff_and_worker_record_count(
            col_name="wkrrecs_percentage_diff",
            min_abs_diff_pct=MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    )


def total_staff_diff_or_total_staff_pct_diff_within_tolerated_range():
    return (
        column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
            col_name="totalstaff_diff",
            min_abs_diff=MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    ) | (
        column_value_is_less_than_min_abs_pct_difference_between_total_staff_and_worker_record_count(
            col_name="totalstaff_percentage_diff",
            min_abs_diff_pct=MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
        )
    )


def calculate_jobcount_handle_tiny_values(input_df):
    # total_staff or worker_record_count < 3: return max
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated('job_count')
                    &
                    total_staff_or_worker_record_count_less_than_permitted_minimum()

            ),
            select_the_bigger_value_between_total_staff_and_worker_rec_count(),
        ).otherwise(F.col("job_count")),
    )


def select_the_bigger_value_between_total_staff_and_worker_rec_count():
    return F.greatest(F.col("total_staff"), F.col("worker_record_count"))


def total_staff_or_worker_record_count_less_than_permitted_minimum():
    return ((F.col("total_staff") < MIN_TOTAL_STAFF_VALUE_PERMITTED)
            | (F.col("worker_record_count") < MIN_WORKER_RECORD_COUNT_PERMITTED))


def calculate_jobcount_abs_difference_within_range(input_df):
    # Abs difference between total_staff & worker_record_count < 5 or < 10% take average:
    input_df = input_df.withColumn(
        "abs_difference", F.abs(input_df.total_staff - input_df.worker_record_count)
    )

    input_df = input_df.withColumn(
        "job_count",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated('job_count')
                    & (

                            column_value_is_less_than_min_abs_difference_between_total_staff_and_worker_record_count(
                                col_name='abs_difference',
                                min_abs_diff=MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT)

                            |
                            mean_abs_difference_less_than_min_pct_difference(abs_dff_col='abs_difference',
                                                                             comparison_col='total_staff',
                                                                             min_diff_val=MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT)

                    )
            ),
            (F.col("total_staff") + F.col("worker_record_count")) / 2,
        ).otherwise(F.col("job_count")),
    )

    input_df = input_df.drop("abs_difference")

    return input_df


def mean_abs_difference_less_than_min_pct_difference(abs_dff_col: str, comparison_col: str, min_diff_val: float):
    return F.col(abs_dff_col) / F.col(comparison_col) < min_diff_val


def calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df):
    # Either worker_record_count or total_staff is null: return first not null
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated('job_count')
                    & (
                            (
                                    selected_column_is_null(col_name='total_staff')
                                    & selected_column_is_not_null(col_name="worker_record_count")
                            )
                            | (
                                    selected_column_is_not_null(col_name='total_staff')
                                    & selected_column_is_null(col_name='worker_record_count')
                            )
                    )
            ),
            select_the_non_null_value_of_total_staff_and_worker_record_count(input_df),
        ).otherwise(F.coalesce(F.col("job_count"))),
    )


def select_the_non_null_value_of_total_staff_and_worker_record_count(input_df):
    return F.coalesce(input_df.total_staff, input_df.worker_record_count)


def selected_column_is_not_null(col_name:str):
    return F.col(col_name).isNotNull()


def selected_column_is_null(col_name: str):
    return F.col(col_name).isNull()


def calculate_jobcount_totalstaff_equal_wkrrecs(input_df):
    # total_staff = wkrrrecs: Take total_staff
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                    job_count_from_ascwds_is_not_populated('job_count')
                    &
                    two_cols_are_equal_and_not_null(first_col='worker_record_count', second_col='total_staff')

            ),
            F.col("total_staff"),
        ).otherwise(F.col("job_count")),
    )


def two_cols_are_equal_and_not_null(first_col:str, second_col: str):
    return ((F.col(first_col) == F.col(second_col))
            & F.col(second_col).isNotNull()
            & F.col(first_col).isNotNull()
            )
