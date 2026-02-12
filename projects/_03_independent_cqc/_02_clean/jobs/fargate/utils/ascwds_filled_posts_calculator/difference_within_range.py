import polars as pl

from projects._03_independent_cqc._02_clean.jobs.fargate.utils.ascwds_filled_posts_calculator.utils import (
    absolute_difference_between_total_staff_and_worker_records_below_cut_off,
    add_source_description_to_source_column,
    ascwds_filled_posts_is_null,
    average_of_two_columns,
    percentage_difference_between_total_staff_and_worker_records_below_cut_off,
    selected_column_is_at_least_the_min_permitted_value,
)
from utils.column_values.categorical_column_values import (
    ASCWDSFilledPostsSource as Source,
)

ascwds_filled_posts_difference_within_range_source_description = (
    Source.average_of_total_staff_and_worker_records
)


def calculate_ascwds_filled_posts_difference_within_range(
    lf: pl.LazyFrame,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
    source_output_column_name: str,
) -> pl.LazyFrame:
    """
    When total staff and worker record values are within an acceptable range of each other, use the average as the output value.
    This function checks that both the total staff and worker record values are at least the minimum permitted value and are within an
    acceptable range of each other, in either absolute or percentage terms. The source column is updated to identify that the average
    value was used as both values were similar.
    Args:
        lf (pl.LazyFrame): The input polars LazyFrame containing total staff and worker record values.
        total_staff_column (str): The name of the column representing the total number of staff.
        worker_records_column (str): The name of the column representing the worker record count.
        output_column_name (str): The name of the column to store the calculated filled posts.
        source_output_column_name (str): The name of the column to store the source of the calculated filled post output.
    Returns:
        pl.LazyFrame: The polars LazyFrame with the calculated filled posts and source columns added.
    """
    lf = lf.with_columns(
        pl.when(
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
            )
        )
        .then(average_of_two_columns(total_staff_column, worker_records_column))
        .otherwise(pl.col(output_column_name))
        .alias(output_column_name)
    )

    lf = add_source_description_to_source_column(
        lf,
        output_column_name,
        source_output_column_name,
        ascwds_filled_posts_difference_within_range_source_description,
    )

    return lf
