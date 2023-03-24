import pyspark.sql
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SNAPSHOT_DATE,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS = 90


def model_non_res_rolling_average(
    df: DataFrame,
) -> DataFrame:
    df.show()
    non_residential_df = df.where(df.primary_service_type == "non-residential")
    non_residential_df.show()

    df_all_dates = non_residential_df.withColumn("snapshot_timestamp", F.to_timestamp(non_residential_df.snapshot_date))

    df_all_dates = df_all_dates.orderBy(df_all_dates.snapshot_timestamp)
    df_all_dates.persist()

    rolling_avg_window = F.window(
        F.col("snapshot_timestamp"),
        windowDuration="91 days",
        slideDuration="1 days",
    ).alias("window")

    rolling_avg = (
        df_all_dates.groupBy(rolling_avg_window)
        .agg(F.avg(F.col("job_count")).alias("model_non_res_rolling_average"))
        .withColumn("snapshot_date", F.date_sub(F.col("window.end").cast("date"), 1))
    ).drop("window")
    # it needs to join with the original data frame here and then we want to duplicate into estimate jobs where necessary, then add the column with the source
    # will need to remove care home df throughout i think
    df_with_rolling_average = df.join(rolling_avg, "snapshot_date", how="left")

    df_with_rolling_average.show()

    df_with_rolling_average = df_with_rolling_average.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (F.col(ESTIMATE_JOB_COUNT).isNull() & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")),
            F.col("model_non_res_rolling_average"),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df_with_rolling_average = update_dataframe_with_identifying_rule(
        df_with_rolling_average, "model_non_res_rolling_average", ESTIMATE_JOB_COUNT
    )
    df_with_rolling_average.show()

    return df_with_rolling_average
