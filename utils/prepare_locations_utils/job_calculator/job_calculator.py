import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType

from utils.prepare_locations_utils.job_calculator.calculate_jobcount_abs_difference_within_range import (
    calculate_jobcount_abs_difference_within_range,
)
from utils.prepare_locations_utils.job_calculator.calculate_job_count_return_only_permitted_value import (
    calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted,
)
from utils.prepare_locations_utils.job_calculator.calculate_jobcount_estimate_from_beds import (
    calculate_jobcount_estimate_from_beds,
)
from utils.prepare_locations_utils.job_calculator.calculate_job_count_return_worker_record_count_if_equal_to_total_staff import (
    calculate_jobcount_totalstaff_equal_wkrrecs,
)


def calculate_jobcount(
    input_df,
    total_staff_column: str,
    worker_records_column: str,
    output_column_name: str,
):
    source_output_column_name = output_column_name + "_source"

    print("Calculating job_count...")

    input_df = input_df.withColumn(output_column_name, F.lit(None).cast(IntegerType()))
    input_df = input_df.withColumn(
        source_output_column_name, F.lit(None).cast(StringType())
    )

    input_df = calculate_jobcount_totalstaff_equal_wkrrecs(
        input_df, total_staff_column, worker_records_column, output_column_name
    )
    input_df = update_dataframe_with_identifying_rule(
        input_df, "worker_records_equal_to_total_staff", output_column_name
    )

    input_df = calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted(
        input_df, worker_records_column, total_staff_column, output_column_name
    )
    input_df = update_dataframe_with_identifying_rule(
        input_df, "worker_records_only_permitted_value", output_column_name
    )

    input_df = calculate_jobcount_select_only_value_which_is_at_least_minimum_job_count_permitted(
        input_df, total_staff_column, worker_records_column, output_column_name
    )
    input_df = update_dataframe_with_identifying_rule(
        input_df, "total_staff_only_permitted_value", output_column_name
    )

    input_df = calculate_jobcount_abs_difference_within_range(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "abs_difference_within_range", output_column_name
    )

    input_df = calculate_jobcount_estimate_from_beds(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "estimate_from_beds", output_column_name
    )

    return input_df


def update_dataframe_with_identifying_rule(input_df, rule_name, output_column_name):
    source_output_column_name = output_column_name + "_source"
    return input_df.withColumn(
        source_output_column_name,
        F.when(
            (
                F.col(output_column_name).isNotNull()
                & F.col(source_output_column_name).isNull()
            ),
            rule_name,
        ).otherwise(F.col(source_output_column_name)),
    )
