import pyspark.sql.functions as F

from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
    selected_column_is_null,
    selected_ascwds_job_count_is_at_least_the_min_permitted,
    selected_ascwds_job_count_is_below_the_min_permitted,
)


def calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted(
    input_df, permitted_column: str, non_permitted_column: str, output_column_name
):
    return input_df.withColumn(
        output_column_name,
        F.when(
            (
                job_count_from_ascwds_is_not_populated(output_column_name)
                & selected_ascwds_job_count_is_at_least_the_min_permitted(
                    permitted_column
                )
                & (
                    selected_column_is_null(non_permitted_column)
                    | selected_ascwds_job_count_is_below_the_min_permitted(
                        non_permitted_column
                    )
                )
            ),
            F.col(permitted_column),
        ).otherwise(F.col(output_column_name)),
    )
