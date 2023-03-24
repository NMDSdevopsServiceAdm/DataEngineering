from pyspark.sql import DataFrame
import pyspark.sql.functions as F


from utils.estimate_job_count.column_names import (
    SNAPSHOT_DATE,
    JOB_COUNT,
)


def calculate_rolling_averages(df: DataFrame, model_name: str, time_period: str, slide: str, days: int) -> DataFrame:

    df_all_dates = df.withColumn("snapshot_timestamp", F.to_timestamp(df.snapshot_date))

    df_all_dates = df_all_dates.orderBy(df_all_dates.snapshot_timestamp)
    df_all_dates.persist()

    rolling_avg_window = F.window(
        F.col("snapshot_timestamp"),
        windowDuration=time_period,
        slideDuration=slide,
    ).alias("window")

    rolling_avg = (
        df_all_dates.groupBy(rolling_avg_window)
        .agg(F.avg(F.col(JOB_COUNT)).alias(model_name))
        .withColumn(
            SNAPSHOT_DATE,
            F.date_sub(F.col("window.end").cast("date"), days),
        )
    ).drop("window")
    return rolling_avg
