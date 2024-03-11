from pyspark.sql import functions as F

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.common_checks import (
    ascwds_filled_posts_is_null,
    column_value_is_less_than_min_absolute_difference_between_total_staff_and_worker_record_count,
    selected_column_is_at_least_the_min_permitted_value,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculation_constants import (
    ASCWDSFilledPostCalculationConstants as calculation_constant,
)
from utils.ind_cqc_filled_posts_utils.utils import (
    update_dataframe_with_identifying_rule,
)

absolute_difference = "absolute_difference"


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
                    column_value_is_less_than_min_absolute_difference_between_total_staff_and_worker_record_count(
                        col_name=absolute_difference,
                        min_abs_diff=calculation_constant.MIN_ABSOLUTE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
                    )
                    | mean_absolute_difference_less_than_min_pct_difference(
                        abs_dff_col=absolute_difference,
                        comparison_col=total_staff_column,
                        min_diff_val=calculation_constant.MIN_PERCENTAGE_DIFFERENCE_BETWEEN_TOTAL_STAFF_AND_WORKER_RECORD_COUNT,
                    )
                )
            ),
            (F.col(total_staff_column) + F.col(worker_records_column)) / 2,
        ).otherwise(F.col(output_column_name)),
    )

    input_df = input_df.drop(absolute_difference)

    input_df = update_dataframe_with_identifying_rule(
        input_df,
        "average of total staff and worker records as both were similar",
        output_column_name,
    )

    return input_df


def mean_absolute_difference_less_than_min_pct_difference(
    abs_dff_col: str, comparison_col: str, min_diff_val: float
):
    return F.col(abs_dff_col) / F.col(comparison_col) < min_diff_val
