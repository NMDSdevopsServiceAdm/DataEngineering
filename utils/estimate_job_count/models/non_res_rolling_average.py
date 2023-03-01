import pyspark.sql
from pyspark.sql import Window
import pyspark.sql.functions as F

from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SNAPSHOT_DATE,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    PRIMARY_SERVICE_TYPE,
)
from utils.estimate_job_count.insert_predictions_into_locations import (
    insert_predictions_into_locations,
)
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS = 90


def model_non_res_rolling_average(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    non_res_rolling_average_df = create_non_res_rolling_average_column(
        df, number_of_days=ROLLING_AVERAGE_TIME_PERIOD_IN_DAYS
    )

    df = insert_predictions_into_locations(
        df, non_res_rolling_average_df, "model_non_res_rolling_average"
    )

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            (
                F.col(ESTIMATE_JOB_COUNT).isNull()
                & (F.col(PRIMARY_SERVICE_TYPE) == "non-residential")
            ),
            F.col("model_non_res_rolling_average"),
        ).otherwise(F.col(ESTIMATE_JOB_COUNT)),
    )

    df = update_dataframe_with_identifying_rule(
        df, "model_non_res_rolling_average", ESTIMATE_JOB_COUNT
    )

    return df.drop("snapshot_date_unix_conv")


def convert_date_to_unix_timestamp(
    df: pyspark.sql.DataFrame, date_col: str, date_format: str, new_col_name: str
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        new_col_name, F.unix_timestamp(F.col(date_col), format=date_format)
    )

    return df


def rolling_average(col_to_average: str, unix_date_col: str, number_of_days: int):
    return F.avg(col_to_average).over(
        rolling_average_time_period(unix_date_col, number_of_days)
    )


def rolling_average_time_period(unix_date_col: str, number_of_days: int):
    return Window.orderBy(F.col(unix_date_col).cast("long")).rangeBetween(
        -convert_days_to_unix_time(number_of_days), 0
    )


def convert_days_to_unix_time(days: int):
    return days * 86400


def create_non_res_rolling_average_column(
    df: pyspark.sql.DataFrame, number_of_days: int
) -> pyspark.sql.DataFrame:
    non_res_rolling_average_df = df.filter(
        F.col(PRIMARY_SERVICE_TYPE) == "non-residential"
    ).select(LOCATION_ID, SNAPSHOT_DATE, JOB_COUNT)

    non_res_rolling_average_df = convert_date_to_unix_timestamp(
        non_res_rolling_average_df,
        date_col=SNAPSHOT_DATE,
        date_format="yyyy-MM-dd",
        new_col_name="snapshot_date_unix_conv",
    )

    non_res_rolling_average_df = non_res_rolling_average_df.withColumn(
        "prediction",
        rolling_average(JOB_COUNT, "snapshot_date_unix_conv", number_of_days),
    )

    return non_res_rolling_average_df
