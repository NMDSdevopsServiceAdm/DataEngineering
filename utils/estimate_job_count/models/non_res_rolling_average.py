# import pyspark.sql
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from dataclasses import dataclass

# from pyspark.sql.types import DoubleType

from utils.estimate_job_count.column_names import (
    SNAPSHOT_DATE,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)

from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)


@dataclass
class NonResRollingAverage:
    ROLLING_AVERAGE_TIME_PERIOD: str = "91 days"
    ROLLING_AVERAGE_WINDOW_SLIDE: str = "1 days"

    NON_RESIDENTIAL: str = "non-residential"
    SNAPSHOT_TIMESTAMP: str = "snapshot_timestamp"
    WINDOW: str = "window"
    WINDOW_END: str = "window.end"
    MODEL_NAME: str = "model_non_res_rolling_average"

    DATE_TYPE: str = "date"
    DAYS: int = 1
    LEFT_JOIN: str = "left"


def model_non_res_rolling_average(
    df: DataFrame,
) -> DataFrame:

    df_with_rolling_average = add_non_residential_rolling_average_column(df)

    df_with_rolling_average = remove_rolling_average_from_care_home_rows(df_with_rolling_average)
    df_with_rolling_average = fill_missing_estimate_job_counts(df_with_rolling_average)

    df_with_rolling_average = update_dataframe_with_identifying_rule(
        df_with_rolling_average, NonResRollingAverage.MODEL_NAME, ESTIMATE_JOB_COUNT
    )

    return df_with_rolling_average


def add_non_residential_rolling_average_column(df: DataFrame) -> DataFrame:
    non_residential_df = df.where(df.primary_service_type == NonResRollingAverage.NON_RESIDENTIAL)
    df_all_dates = non_residential_df.withColumn(
        NonResRollingAverage.SNAPSHOT_TIMESTAMP, F.to_timestamp(non_residential_df.snapshot_date)
    )

    df_all_dates = df_all_dates.orderBy(df_all_dates.snapshot_timestamp)
    df_all_dates.persist()

    rolling_avg_window = F.window(
        F.col(NonResRollingAverage.SNAPSHOT_TIMESTAMP),
        windowDuration=NonResRollingAverage.ROLLING_AVERAGE_TIME_PERIOD,
        slideDuration=NonResRollingAverage.ROLLING_AVERAGE_WINDOW_SLIDE,
    ).alias(NonResRollingAverage.WINDOW)

    rolling_avg = (
        df_all_dates.groupBy(rolling_avg_window)
        .agg(F.avg(F.col(JOB_COUNT)).alias(NonResRollingAverage.MODEL_NAME))
        .withColumn(
            SNAPSHOT_DATE,
            F.date_sub(
                F.col(NonResRollingAverage.WINDOW_END).cast(NonResRollingAverage.DATE_TYPE), NonResRollingAverage.DAYS
            ),
        )
    ).drop(NonResRollingAverage.WINDOW)

    df = df.join(rolling_avg, SNAPSHOT_DATE, how=NonResRollingAverage.LEFT_JOIN)
    return df


def remove_rolling_average_from_care_home_rows(df: DataFrame) -> DataFrame:
    care_home_df = df.where(df.primary_service_type != NonResRollingAverage.NON_RESIDENTIAL)
    non_residential_df = df.where(df.primary_service_type == NonResRollingAverage.NON_RESIDENTIAL)
    care_home_df = care_home_df.withColumn(NonResRollingAverage.MODEL_NAME, F.lit(None))
    df = non_residential_df.union(care_home_df)
    return df


def fill_missing_estimate_job_counts(df: DataFrame) -> DataFrame:
    rows_to_fill_df = df.where(
        (F.col(ESTIMATE_JOB_COUNT).isNull()) & (F.col(PRIMARY_SERVICE_TYPE) == NonResRollingAverage.NON_RESIDENTIAL)
    )
    rows_not_to_fill = df.where(
        (F.col(ESTIMATE_JOB_COUNT).isNotNull()) | (F.col(PRIMARY_SERVICE_TYPE) != NonResRollingAverage.NON_RESIDENTIAL)
    )
    rows_to_fill_df = rows_to_fill_df.withColumn(ESTIMATE_JOB_COUNT, F.col(NonResRollingAverage.MODEL_NAME))
    df = rows_to_fill_df.union(rows_not_to_fill)
    return df
