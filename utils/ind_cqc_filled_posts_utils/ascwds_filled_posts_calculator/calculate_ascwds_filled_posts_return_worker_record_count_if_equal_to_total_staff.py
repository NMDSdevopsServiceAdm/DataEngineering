from pyspark.sql import DataFrame, functions as F

from utils.column_values.categorical_column_values import (
    ASCWDSFilledPostsSource as Source,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.utils import (
    ascwds_filled_posts_is_null,
    two_cols_are_equal_and_at_least_minimum_permitted_value,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    add_source_description_to_source_column,
)

ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description = (
    Source.worker_records_and_total_staff
)


def calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
    df: DataFrame,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
    source_output_column_name: str,
) -> DataFrame:
    """
    When total staff and worker record values match, use that value as the output value.

    This function enters the worker record value when the total staff and worker record values match each other and are
    at least the minimum permitted value. The source column is updated to identify that both values matched.

    Args:
        df (DataFrame): The input DataFrame containing total staff and worker record values.
        total_staff_column (str): The name of the column representing the total number of staff.
        worker_records_column (str): The name of the column representing the worker record count.
        output_column_name (str): The name of the column to store the calculated filled posts.
        source_output_column_name (str): The name of the column to store the source of the calculated filled post output.

    Returns:
        DataFrame: The DataFrame with the calculated filled posts and source columns added.
    """
    df = df.withColumn(
        output_column_name,
        F.when(
            (
                ascwds_filled_posts_is_null()
                & two_cols_are_equal_and_at_least_minimum_permitted_value(
                    total_staff_column, worker_records_column
                )
            ),
            F.col(worker_records_column),
        ).otherwise(F.col(output_column_name)),
    )

    df = add_source_description_to_source_column(
        df,
        output_column_name,
        source_output_column_name,
        ascwds_filled_posts_totalstaff_equal_wkrrecs_source_description,
    )
    return df
