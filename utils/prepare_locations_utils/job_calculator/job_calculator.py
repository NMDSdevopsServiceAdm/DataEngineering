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
from utils.prepare_locations_utils.job_calculator.calculate_jobcount_total_staff_equal_worker_records import (
    calculate_jobcount_totalstaff_equal_wkrrecs,
)


def calculate_jobcount(input_df):
    print("Calculating job_count...")

    input_df = input_df.withColumn("job_count", F.lit(None).cast(IntegerType()))
    input_df = input_df.withColumn("job_count_source", F.lit(None).cast(StringType()))

    input_df = calculate_jobcount_totalstaff_equal_wkrrecs(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "totalstaff_equal_wkrrecs"
    )

    input_df = calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "coalesce_total_staff_wkrrecs"
    )

    input_df = calculate_jobcount_abs_difference_within_range(input_df)
    input_df = update_dataframe_with_identifying_rule(
        input_df, "abs_difference_within_range"
    )

    input_df = calculate_jobcount_handle_tiny_values(input_df)
    input_df = update_dataframe_with_identifying_rule(input_df, "handle_tiny_values")

    input_df = calculate_jobcount_estimate_from_beds(input_df)
    input_df = update_dataframe_with_identifying_rule(input_df, "estimate_from_beds")

    return input_df


def update_dataframe_with_identifying_rule(input_df, rule_name):
    return input_df.withColumn(
        "job_count_source",
        F.when(
            (F.col("job_count").isNotNull() & F.col("job_count_source").isNull()),
            rule_name,
        ).otherwise(F.col("job_count_source")),
    )
