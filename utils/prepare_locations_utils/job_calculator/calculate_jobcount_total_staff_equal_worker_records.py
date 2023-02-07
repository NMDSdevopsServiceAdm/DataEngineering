from utils.prepare_locations_utils.job_calculator.common_checks import (
    job_count_from_ascwds_is_not_populated,
)
from utils.prepare_locations_utils.job_calculator.calculate_jobcount_estimate_from_beds import (
    two_cols_are_equal_and_not_null,
)
import pyspark.sql.functions as F


def calculate_jobcount_totalstaff_equal_wkrrecs(input_df):
    # total_staff = wkrrrecs: Take total_staff
    return input_df.withColumn(
        "job_count",
        F.when(
            (
                job_count_from_ascwds_is_not_populated("job_count")
                & two_cols_are_equal_and_not_null(
                    first_col="worker_record_count", second_col="total_staff"
                )
            ),
            F.col("total_staff"),
        ).otherwise(F.col("job_count")),
    )
