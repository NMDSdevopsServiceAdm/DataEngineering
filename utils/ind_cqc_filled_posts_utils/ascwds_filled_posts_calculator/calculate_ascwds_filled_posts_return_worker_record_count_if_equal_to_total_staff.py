from pyspark.sql import DataFrame, functions as F

from utils.column_values.categorical_column_values import (
    ASCWDSFilledPostsSource as Source,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
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
