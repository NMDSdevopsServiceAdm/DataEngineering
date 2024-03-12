from pyspark.sql import functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_column_is_at_least_the_min_permitted_value,
    absolute_difference_between_total_staff_and_worker_records_below_cut_off,
    percentage_difference_between_total_staff_and_worker_records_below_cut_off,
    average_of_two_columns,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    update_dataframe_with_identifying_rule,
)

ascwds_filled_posts_absolute_difference_within_range_source_description: str = (
    "average of total staff and worker records as both were similar"
)


def calculate_ascwds_filled_posts_absolute_difference_within_range(
    input_df, total_staff_column: str, worker_records_column: str, output_column_name
):
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
                    absolute_difference_between_total_staff_and_worker_records_below_cut_off()
                    | percentage_difference_between_total_staff_and_worker_records_below_cut_off()
                )
            ),
            average_of_two_columns(total_staff_column, worker_records_column),
        ).otherwise(F.col(output_column_name)),
    )

    return update_dataframe_with_identifying_rule(
        input_df,
        ascwds_filled_posts_absolute_difference_within_range_source_description,
        output_column_name,
    )
