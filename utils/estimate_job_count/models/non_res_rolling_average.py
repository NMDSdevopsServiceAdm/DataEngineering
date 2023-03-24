import pyspark.sql
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

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

    locations_df_single_run = df
    locations_df_single_run = locations_df_single_run.where(
        locations_df_single_run.primary_service_type == "non-residential"
    )

    df_all_dates = locations_df_single_run.withColumn(
        "snapshot_timestamp", F.to_timestamp(locations_df_single_run.snapshot_date)
    )

    df_all_dates = df_all_dates.orderBy(df_all_dates.snapshot_timestamp)
    df_all_dates.persist()
    df_all_dates.show()

    rolling_avg_window = F.window(
        F.col("snapshot_timestamp"),
        windowDuration="91 days",
        slideDuration="1 days",
    ).alias("window")

    rolling_avg = (
        df_all_dates.groupBy(rolling_avg_window)
        .agg(F.avg(F.col("job_count")).alias("model_non_res_rolling_average"))
        .withColumn("snapshot_date", F.date_sub(F.col("window.end").cast("date"), 1))
    )

    df = locations_df_single_run.join(rolling_avg, "snapshot_date", how="left")

    df = update_dataframe_with_identifying_rule(df, "model_non_res_rolling_average", ESTIMATE_JOB_COUNT)

    return df.drop("snapshot_date_unix_conv")
