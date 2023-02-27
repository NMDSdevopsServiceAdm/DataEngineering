import pyspark.sql
from pyspark.sql import Window
import pyspark.sql.functions as F

from utils.estimate_job_count.column_names import (
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)


def model_non_res_rolling_three_month_average(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    """
    Non-res : Not Historical : Not PIR : rolling 3 month average of job_count based on snapshot_date
    """
    df = convert_date_to_unix_timestamp(
        df,
        date_col="snapshot_date",
        date_format="yyyy-MM-dd",
        new_col_name="snapshot_date_unix_conv",
    )

    df = df.withColumn(
        "model_non_res_rolling_three_month_average",
        F.when(
            (F.col(PRIMARY_SERVICE_TYPE) == "non-residential"),
            rolling_average(
                "job_count", "primary_service_type", "snapshot_date_unix_conv", 90
            ),
        ),
    )

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
            ),
            F.col("model_non_res_rolling_three_month_average"),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df = update_dataframe_with_identifying_rule(
        df, "model_non_res_rolling_three_month_average", ESTIMATE_JOB_COUNT
    )

    return df.drop("snapshot_date_unix_conv")


def convert_date_to_unix_timestamp(
    df, date_col: str, date_format: str, new_col_name: str
):
    df = df.withColumn(
        new_col_name, F.unix_timestamp(F.col(date_col), format=date_format)
    )

    return df


def partition_and_time_period(
    partition_col: str, unix_date_col: str, number_of_days: int
):
    return (
        Window.partitionBy(F.col(partition_col))
        .orderBy(F.col(unix_date_col).cast("long"))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )


def rolling_average(
    col_to_average: str, partition_col: str, unix_date_col: str, number_of_days: int
):
    return F.avg(col_to_average).over(
        partition_and_time_period(partition_col, unix_date_col, number_of_days)
    )


def convert_days_to_unix_time(days: int):
    return days * 86400
