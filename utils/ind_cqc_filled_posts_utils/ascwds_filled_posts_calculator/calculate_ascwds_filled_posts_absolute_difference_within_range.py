from pyspark.sql import DataFrame, functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_column_is_at_least_the_min_permitted_value,
    absolute_difference_between_total_staff_and_worker_records_below_cut_off,
    percentage_difference_between_total_staff_and_worker_records_below_cut_off,
    average_of_two_columns,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    add_source_description_to_source_column,
)

ascwds_filled_posts_absolute_difference_within_range_source_description: str = (
    "average of total staff and worker records as both were similar"
)


def calculate_ascwds_filled_posts_absolute_difference_within_range(
    input_df: DataFrame,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
    source_output_column_name: str,
) -> DataFrame:
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

    return add_source_description_to_source_column(
        input_df,
        output_column_name,
        source_output_column_name,
        ascwds_filled_posts_absolute_difference_within_range_source_description,
    )
