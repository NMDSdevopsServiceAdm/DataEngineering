import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from utils.prepare_locations_utils.job_calculator.job_estimate_calculator import \
    calculate_jobcount_totalstaff_equal_wkrrecs, \
    calculate_jobcount_coalesce_totalstaff_wkrrecs, calculate_jobcount_abs_difference_within_range, \
    calculate_jobcount_handle_tiny_values, calculate_jobcount_estimate_from_beds


# TODO: write test cases for this so we are testing how this logic works together
def calculate_jobcount(input_df):
    print("Calculating job_count...")

    # Add null/empty job_count column
    input_df = input_df.withColumn("job_count", F.lit(None).cast(IntegerType()))

    input_df = calculate_jobcount_totalstaff_equal_wkrrecs(input_df)
    input_df = calculate_jobcount_coalesce_totalstaff_wkrrecs(input_df)
    input_df = calculate_jobcount_abs_difference_within_range(input_df)
    input_df = calculate_jobcount_handle_tiny_values(input_df)
    input_df = calculate_jobcount_estimate_from_beds(input_df)

    return input_df
