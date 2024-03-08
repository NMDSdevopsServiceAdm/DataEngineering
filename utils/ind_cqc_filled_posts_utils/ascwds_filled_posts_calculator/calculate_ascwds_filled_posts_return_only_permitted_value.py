import pyspark.sql.functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_column_is_null,
    selected_column_is_at_least_the_min_permitted_value,
    selected_column_is_below_the_min_permitted_value,
)


def calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_job_count_permitted(
    input_df, permitted_column: str, non_permitted_column: str, output_column_name
):
    return input_df.withColumn(
        output_column_name,
        F.when(
            (
                ascwds_filled_posts_is_null()
                & selected_column_is_at_least_the_min_permitted_value(permitted_column)
                & (
                    selected_column_is_null(non_permitted_column)
                    | selected_column_is_below_the_min_permitted_value(
                        non_permitted_column
                    )
                )
            ),
            F.col(permitted_column),
        ).otherwise(F.col(output_column_name)),
    )
