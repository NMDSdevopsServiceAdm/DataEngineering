from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils.estimate_job_count.column_names import (
    SNAPSHOT_DATE,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

ROLLING_AVERAGE_TIME_PERIOD = "91 days"
ROLLING_AVERAGE_WINDOW_SLIDE = "1 days"

NON_RESIDENTIAL = "non-residential"
SNAPSHOT_TIMESTAMP = "snapshot_timestamp"
WINDOW = "window"
WINDOW_END = "window.end"
MODEL_NAME = "model_non_res_rolling_average"

LEFT_JOIN = "left"
DATE_TYPE = "date"
ONE_DAY = 1


def model_non_res_rolling_average(
    df: DataFrame,
) -> DataFrame:
    df.show()
    non_residential_df = df.where(df.primary_service_type == NON_RESIDENTIAL)
    non_residential_df.show()

    df_all_dates = non_residential_df.withColumn(SNAPSHOT_TIMESTAMP, F.to_timestamp(non_residential_df.snapshot_date))

    df_all_dates = df_all_dates.orderBy(df_all_dates.snapshot_timestamp)
    df_all_dates.persist()

    rolling_avg_window = F.window(
        F.col(SNAPSHOT_TIMESTAMP),
        windowDuration=ROLLING_AVERAGE_TIME_PERIOD,
        slideDuration=ROLLING_AVERAGE_WINDOW_SLIDE,
    ).alias(WINDOW)

    rolling_avg = (
        df_all_dates.groupBy(rolling_avg_window)
        .agg(F.avg(F.col(JOB_COUNT)).alias(MODEL_NAME))
        .withColumn(SNAPSHOT_DATE, F.date_sub(F.col(WINDOW_END).cast(DATE_TYPE), ONE_DAY))
    ).drop(WINDOW)

    df_with_rolling_average = df.join(rolling_avg, SNAPSHOT_DATE, how=LEFT_JOIN)

    df_with_rolling_average.show()

    df_with_rolling_average = df_with_rolling_average.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (F.col(ESTIMATE_JOB_COUNT).isNull() & (F.col(PRIMARY_SERVICE_TYPE) == NON_RESIDENTIAL)),
            F.col(MODEL_NAME),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df_with_rolling_average = update_dataframe_with_identifying_rule(
        df_with_rolling_average, MODEL_NAME, ESTIMATE_JOB_COUNT
    )
    df_with_rolling_average.show()

    return df_with_rolling_average
