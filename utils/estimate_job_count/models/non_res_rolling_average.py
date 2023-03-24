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

    non_residential_df = df.where(df.primary_service_type == "non-residential")

    care_home_df = df.where(df.primary_service_type != "non-residential")
    care_home_df = care_home_df.withColumn("model_non_res_rolling_average", F.lit(None).cast(DoubleType()))

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

    non_residential_df_with_rolling_average = non_residential_df.join(rolling_avg, "snapshot_date", how="left")
    non_residential_df_with_rolling_average.show()
    df_with_rolling_average = non_residential_df_with_rolling_average.union(care_home_df)
    df_with_rolling_average.show()
    df_with_rolling_average = update_dataframe_with_identifying_rule(
        df_with_rolling_average, "model_non_res_rolling_average", ESTIMATE_JOB_COUNT
    )
    df_with_rolling_average.show()
    df_with_rolling_average = df_with_rolling_average.drop("snapshot_timestamp")
    df_with_rolling_average.show()
    return df_with_rolling_average
