from pyspark.sql import DataFrame, functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_column_is_at_least_the_min_permitted_value,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    add_source_description_to_source_column,
)


def ascwds_filled_posts_select_only_value_source_description(permitted_column: str):
    return "only " + permitted_column + " was provided"


def calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value(
    df: DataFrame,
    permitted_column: str,
    non_permitted_column: str,
    output_column_name: str,
    source_output_column_name: str,
) -> DataFrame:
    df = df.withColumn(
        output_column_name,
        F.when(
            (
                ascwds_filled_posts_is_null()
                & selected_column_is_at_least_the_min_permitted_value(permitted_column)
            ),
            F.col(permitted_column),
        ).otherwise(F.col(output_column_name)),
    )

    df = add_source_description_to_source_column(
        df,
        output_column_name,
        source_output_column_name,
        ascwds_filled_posts_select_only_value_source_description(permitted_column),
    )
    return df
