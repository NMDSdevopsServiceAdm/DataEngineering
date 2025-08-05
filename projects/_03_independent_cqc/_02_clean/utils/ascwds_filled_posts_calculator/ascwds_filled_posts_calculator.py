from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame, functions as F

from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.difference_within_range import (
    calculate_ascwds_filled_posts_difference_within_range,
)
from projects._03_independent_cqc._02_clean.utils.ascwds_filled_posts_calculator.total_staff_equals_worker_records import (
    calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs,
)


def calculate_ascwds_filled_posts(
    input_df: DataFrame,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
    source_output_column_name: str,
) -> DataFrame:
    """
    This function calculates the number of filled posts based on the total staff and worker records columns.

    This function calculates the number of filled posts based on the total staff and worker records columns
    and creates two new columns in the DataFrame: one for the output of the filled posts calculation and
    another for the source of this calculation.

    Args:
        input_df (DataFrame): The input DataFrame containing total staff and worker record values.
        total_staff_column (str): The name of the column representing the total number of staff.
        worker_records_column (str): The name of the column representing the worker record count.
        output_column_name (str): The name of the column to store the calculated filled posts.
        source_output_column_name (str): The name of the column to store the source of the calculated filled post output.

    Returns:
        DataFrame: The DataFrame with the calculated filled posts and source columns added.
    """
    print("Calculating ascwds_filled_posts...")

    input_df = input_df.withColumn(output_column_name, F.lit(None).cast(IntegerType()))
    input_df = input_df.withColumn(
        source_output_column_name, F.lit(None).cast(StringType())
    )

    input_df = calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
        input_df,
        total_staff_column,
        worker_records_column,
        output_column_name,
        source_output_column_name,
    )

    input_df = calculate_ascwds_filled_posts_difference_within_range(
        input_df,
        total_staff_column,
        worker_records_column,
        output_column_name,
        source_output_column_name,
    )

    return input_df
