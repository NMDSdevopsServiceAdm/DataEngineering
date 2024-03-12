import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame

from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_absolute_difference_within_range import (
    calculate_ascwds_filled_posts_absolute_difference_within_range,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_only_permitted_value import (
    calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value,
)
from utils.ind_cqc_filled_posts_utils.ascwds_filled_posts_calculator.calculate_ascwds_filled_posts_return_worker_record_count_if_equal_to_total_staff import (
    calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs,
)


def calculate_ascwds_filled_posts(
    input_df: DataFrame,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
) -> DataFrame:
    source_output_column_name = output_column_name + "_source"

    print("Calculating ascwds_filled_posts...")

    input_df = input_df.withColumn(output_column_name, F.lit(None).cast(IntegerType()))
    input_df = input_df.withColumn(
        source_output_column_name, F.lit(None).cast(StringType())
    )

    input_df = calculate_ascwds_filled_posts_totalstaff_equal_wkrrecs(
        input_df, total_staff_column, worker_records_column, output_column_name
    )

    input_df = calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value(
        input_df, worker_records_column, total_staff_column, output_column_name
    )

    input_df = calculate_ascwds_filled_posts_select_only_value_which_is_at_least_minimum_permitted_value(
        input_df, total_staff_column, worker_records_column, output_column_name
    )

    input_df = calculate_ascwds_filled_posts_absolute_difference_within_range(
        input_df, total_staff_column, worker_records_column, output_column_name
    )

    return input_df
