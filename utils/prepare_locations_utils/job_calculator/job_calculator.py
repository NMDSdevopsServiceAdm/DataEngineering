import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType

from utils.prepare_locations_utils.job_calculator.calculate_job_count_for_tiny_values import (
    calculate_jobcount_handle_tiny_values,
)
from utils.prepare_locations_utils.job_calculator.calculate_jobcount_abs_difference_within_range import (
    calculate_jobcount_abs_difference_within_range,
)
from utils.prepare_locations_utils.job_calculator.calculate_jobcount_coalesce_totalstaff_wkrrecs import (
    calculate_jobcount_coalesce_totalstaff_wkrrecs,
)
from utils.prepare_locations_utils.job_calculator.calculate_jobcount_estimate_from_beds import (
    calculate_jobcount_estimate_from_beds,
)
from utils.prepare_locations_utils.job_calculator.calculate_job_count_return_worker_record_count_if_equal_to_total_staff import (
    calculate_jobcount_totalstaff_equal_wkrrecs,
)


def calculate_jobcount(
    input_df, total_staff_column, worker_records_column, output_column_name
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

    input_df = calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "coalesce_total_staff_wkrrecs", output_column_name
    )

    input_df = calculate_jobcount_abs_difference_within_range(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "abs_difference_within_range", output_column_name
    )

    input_df = calculate_jobcount_handle_tiny_values(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "handle_tiny_values", output_column_name
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
