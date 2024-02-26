import pyspark.sql

from utils.prepare_locations_utils.filter_job_count.care_home_jobs_per_bed_ratio_outliers import (
    care_home_jobs_per_bed_ratio_outliers,
)


def null_job_count_outliers(input_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    print("Removing job_count_unfiltered outliers...")

    # TODO: Add back when we have job count columns
    # input_df = care_home_jobs_per_bed_ratio_outliers(input_df)

    return input_df
