from pyspark.sql import functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_column_is_at_least_the_min_permitted_value,
    column_value_is_less_than_max_absolute_difference,
    mean_absolute_difference_less_than_max_pct_difference,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    update_dataframe_with_identifying_rule,
)

absolute_difference: str = "absolute_difference"
ascwds_filled_posts_absolute_difference_within_range_source_description: str = (
    "average of total staff and worker records as both were similar"
)


def calculate_ascwds_filled_posts_absolute_difference_within_range(
    input_df, total_staff_column: str, worker_records_column: str, output_column_name
):
    input_df = input_df.withColumn(
        absolute_difference,
        F.abs(F.col(total_staff_column) - F.col(worker_records_column)),
    )

    input_df = input_df.withColumn(
        output_column_name,
        F.when(
            (
                ascwds_filled_posts_is_null()
                & selected_column_is_at_least_the_min_permitted_value(
                    total_staff_column
                )
                & selected_column_is_at_least_the_min_permitted_value(
                    worker_records_column
                )
                & (
                    column_value_is_less_than_max_absolute_difference(
                        col_name=absolute_difference,
                    )
                    | mean_absolute_difference_less_than_max_pct_difference(
                        abs_dff_col=absolute_difference,
                        comparison_col=total_staff_column,
                    )
                )
            ),
            (F.col(total_staff_column) + F.col(worker_records_column)) / 2,
        ).otherwise(F.col(output_column_name)),
    )

    input_df = input_df.drop(absolute_difference)

    input_df = update_dataframe_with_identifying_rule(
        input_df,
        ascwds_filled_posts_absolute_difference_within_range_source_description,
        output_column_name,
    )

    return input_df
