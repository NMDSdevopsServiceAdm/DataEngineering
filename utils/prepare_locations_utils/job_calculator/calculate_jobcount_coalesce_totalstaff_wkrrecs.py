from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
    selected_column_is_null,
    selected_column_is_not_null,
)

import pyspark.sql.functions as F


def calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df):
    df_with_temp_count = input_df.withColumn(
        "job_count_temp",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count")
                & (
                    (
                        selected_column_is_null(col_name="total_staff")
                        & selected_column_is_not_null(col_name="worker_record_count")
                    )
                    | (
                        selected_column_is_not_null(col_name="total_staff")
                        & selected_column_is_null(col_name="worker_record_count")
                    )
                )
            ),
            select_the_non_null_value_of_total_staff_and_worker_record_count(input_df),
        ),
    )

    df_with_populated_job_count_source = df_with_temp_count.withColumn(
        "job_count_source",
        F.when(F.col("job_count_temp").isNotNull(), "coalesce_total_staff_wkrrecs"),
    )

    df_with_job_count_populated = df_with_populated_job_count_source.withColumn(
        "job_count",
        (
            F.when(
                F.col("job_count_temp").isNotNull(), F.col("job_count_temp")
            ).otherwise(F.coalesce(F.col("job_count")))
        ),
    )
    output_df = df_with_job_count_populated.drop("job_count_temp")
    return output_df


def select_the_non_null_value_of_total_staff_and_worker_record_count(input_df):
    return F.coalesce(input_df.total_staff, input_df.worker_record_count)
