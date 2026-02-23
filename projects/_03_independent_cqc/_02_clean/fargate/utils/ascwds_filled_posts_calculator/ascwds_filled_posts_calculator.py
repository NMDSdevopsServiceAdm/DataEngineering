import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.difference_within_range import (
    calculate_ascwds_filled_posts_difference_within_range,
)
from projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.total_staff_equals_worker_records import (
    calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs,
)


def calculate_ascwds_filled_posts(
    input_lf: pl.LazyFrame,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
    source_output_column_name: str,
) -> pl.LazyFrame:
    """
    This function calculates the number of filled posts based on the total staff and worker records columns.
    This function calculates the number of filled posts based on the total staff and worker records columns
    and creates two new columns in the LazyFrame: one for the output of the filled posts calculation and
    another for the source of this calculation.
    Args:
        input_lf (pl.LazyFrame): The input polars LazyFrame containing total staff and worker record values.
        total_staff_column (str): The name of the column representing the total number of staff.
        worker_records_column (str): The name of the column representing the worker record count.
        output_column_name (str): The name of the column to store the calculated filled posts.
        source_output_column_name (str): The name of the column to store the source of the calculated filled post output.
    Returns:
        pl.LazyFrame: The polars LazyFrame with the calculated filled posts and source columns added.
    """
    print("Calculating ascwds_filled_posts...")

    input_lf = input_lf.with_columns(
        [
            pl.lit(None).cast(pl.Int64).alias(output_column_name),
            pl.lit(None).cast(pl.Utf8).alias(source_output_column_name),
        ]
    )

    input_lf = calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
        input_lf,
        total_staff_column,
        worker_records_column,
        output_column_name,
        source_output_column_name,
    )

    input_lf = calculate_ascwds_filled_posts_difference_within_range(
        input_lf,
        total_staff_column,
        worker_records_column,
        output_column_name,
        source_output_column_name,
    )

    return input_lf
