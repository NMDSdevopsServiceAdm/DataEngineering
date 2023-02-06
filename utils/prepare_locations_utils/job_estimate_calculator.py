from pyspark.sql import functions as F

BEDS_IN_WORKPLACE_THRESHOLD = 0

BEDS_TO_JOB_COUNT_INTERCEPT = 8.40975704621392
BEDS_TO_JOB_COUNT_COEFFICIENT = 1.0010753137758377001
MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT = 5
MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT = 0.1

MIN_TOTAL_STAFF_VALUE_PERMITTED = 3
MIN_WORKER_RECORD_COUNT_PERMITTED = 3


def calculate_jobcount_estimate_from_beds(input_df):
    input_df = input_df.withColumn(
        "bed_estimate_jobcount",
        F.when(
            (
                F.col("job_count").isNull()
                & (F.col("number_of_beds") > BEDS_IN_WORKPLACE_THRESHOLD)
            ),
            (
                BEDS_TO_JOB_COUNT_INTERCEPT
                + (F.col("number_of_beds") * BEDS_TO_JOB_COUNT_COEFFICIENT)
            ),
        ).otherwise(None),
    )

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

    # Bounding predictions to certain locations with differences in range
    # if total_staff and worker_record_count within 10% or < 5: return avg(total_staff + wkrrds)
    input_df = input_df.withColumn(
        "job_count",
        F.when(
            (
                F.col("job_count").isNull()
                & F.col("bed_estimate_jobcount").isNotNull()
                & (
                    (
                        (
                            F.col("totalstaff_diff")
                            < MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                        )
                        | (
                            F.col("totalstaff_percentage_diff")
                            < MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                        )
                    )
                    & (
                        (
                            F.col("wkrrecs_diff")
                            < MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                        )
                        | (
                            F.col("wkrrecs_percentage_diff")
                            < MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                        )
                    )
                )
            ),
            (F.col("total_staff") + F.col("worker_record_count")) / 2,
        ).otherwise(F.col("job_count")),
    )

    # if total_staff within 10% or < 5: return total_staff
    input_df = input_df.withColumn(
        "job_count",
        F.when(
            (
                F.col("job_count").isNull()
                & F.col("bed_estimate_jobcount").isNotNull()
                & (
                    (
                        F.col("totalstaff_diff")
                        < MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                    )
                    | (
                        F.col("totalstaff_percentage_diff")
                        < MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                    )
                )
            ),
            F.col("total_staff"),
        ).otherwise(F.col("job_count")),
    )

    # if worker_record_count within 10% or < 5: return worker_record_count
    input_df = input_df.withColumn(
        "job_count",
        F.when(
            (
                F.col("job_count").isNull()
                & F.col("bed_estimate_jobcount").isNotNull()
                & (
                    (
                        F.col("wkrrecs_diff")
                        < MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                    )
                    | (
                        F.col("wkrrecs_percentage_diff")
                        < MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                    )
                )
            ),
            F.col("worker_record_count"),
        ).otherwise(F.col("job_count")),
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


def calculate_jobcount_handle_tiny_values(input_df):
    # total_staff or worker_record_count < 3: return max
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                F.col("job_count").isNull()
                & (
                    (F.col("total_staff") < MIN_TOTAL_STAFF_VALUE_PERMITTED)
                    | (F.col("worker_record_count") < MIN_WORKER_RECORD_COUNT_PERMITTED)
                )
            ),
            F.greatest(F.col("total_staff"), F.col("worker_record_count")),
        ).otherwise(F.col("job_count")),
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
                F.col("job_count").isNull()
                & (
                    (
                        F.col("abs_difference")
                        < MIN_ABS_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                    )
                    | (
                        F.col("abs_difference") / F.col("total_staff")
                        < MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT
                    )
                )
            ),
            (F.col("total_staff") + F.col("worker_record_count")) / 2,
        ).otherwise(F.col("job_count")),
    )

    input_df = input_df.drop("abs_difference")

    return input_df


def calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df):
    # Either worker_record_count or total_staff is null: return first not null
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                F.col("job_count").isNull()
                & (
                    (
                        F.col("total_staff").isNull()
                        & F.col("worker_record_count").isNotNull()
                    )
                    | (
                        F.col("total_staff").isNotNull()
                        & F.col("worker_record_count").isNull()
                    )
                )
            ),
            F.coalesce(input_df.total_staff, input_df.worker_record_count),
        ).otherwise(F.coalesce(F.col("job_count"))),
    )


def calculate_jobcount_totalstaff_equal_wkrrecs(input_df):
    # total_staff = wkrrrecs: Take total_staff
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                F.col("job_count").isNull()
                & (F.col("worker_record_count") == F.col("total_staff"))
                & F.col("total_staff").isNotNull()
                & F.col("worker_record_count").isNotNull()
            ),
            F.col("total_staff"),
        ).otherwise(F.col("job_count")),
    )
