import pyspark.sql.functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    selected_column_is_null,
    selected_column_is_at_least_the_min_permitted_value,
    selected_column_is_below_the_min_permitted_value,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    update_dataframe_with_identifying_rule,
)

only_one_permitted_value_source_description = (
    "only one of total staff or worker records were provided"
)


def calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value(
    input_df, permitted_column: str, non_permitted_column: str, output_column_name
):
    input_df = input_df.withColumn(
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

    return update_dataframe_with_identifying_rule(
        input_df, "only " + permitted_column + " was provided", output_column_name
    )
