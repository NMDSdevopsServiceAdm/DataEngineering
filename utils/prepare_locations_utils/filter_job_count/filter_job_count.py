import pyspark.sql

from utils.prepare_locations_utils.filter_job_count.care_home_jobs_per_bed_ratio_outliers import (
    care_home_jobs_per_bed_ratio_outliers,
)


def filter_job_count(input_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:

    print("Filtering job_count...")

    input_df = care_home_jobs_per_bed_ratio_outliers(input_df)

    return input_df
