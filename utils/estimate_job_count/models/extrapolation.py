import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
import pyspark.sql


def model_extrapolation(df: DataFrame) -> DataFrame:
    df = convert_date_to_unix_timestamp(
        df,
        date_col="snapshot_date",
        date_format="yyyy-MM-dd",
        new_col_name="unix_time",
    )

    max_job_count_per_location_df = df.groupBy("locationid").agg(
        F.max("job_count").alias("max_job_count")
    )
    max_job_count_per_location_df = max_job_count_per_location_df.where(
        F.col("max_job_count") > 0.0
    )

    for_extrapolation = max_job_count_per_location_df.join(df, "locationid", "left")

    for_extrapolation = for_extrapolation.select(
        "locationid", "snapshot_date", "unix_time", "job_count", "primary_service_type"
    )

    df_with_populated_job_count_only = for_extrapolation.where(
        F.col("job_count").isNotNull()
    )

    average_df = df_with_populated_job_count_only.groupBy(
        "primary_service_type", "unix_time"
    ).agg(
        F.count("job_count").cast("integer").alias("count"),
        F.sum("job_count").alias("total_job_count"),
    )

    average_df = create_rolling_average_column(average_df, number_of_days=88)

    average_df = average_df.select(
        "primary_service_type", "unix_time", "rolling_average"
    )

    df_with_populated_job_count_only = df_with_populated_job_count_only.join(
        average_df, ["primary_service_type", "unix_time"], "left"
    )

    grouped_df = df_with_populated_job_count_only.groupBy("locationid").agg(
        F.min("unix_time").cast("integer").alias("u_time_min"),
        F.max("unix_time").cast("integer").alias("u_time_max"),
    )

    df_all_dates = df_with_populated_job_count_only.join(
        grouped_df, "locationid", "left"
    )

    first_job_count_df = df_all_dates.where(
        df_all_dates.u_time_min == df_all_dates.unix_time
    )
    first_job_count_df = first_job_count_df.withColumnRenamed(
        "job_count", "first_job_count"
    )
    first_job_count_df = first_job_count_df.withColumnRenamed(
        "rolling_average", "first_rolling_average"
    )
    first_job_count_df = first_job_count_df.select(
        "locationid", "first_job_count", "first_rolling_average"
    )

    last_job_count_df = df_all_dates.where(
        df_all_dates.u_time_max == df_all_dates.unix_time
    )
    last_job_count_df = last_job_count_df.withColumnRenamed(
        "job_count", "last_job_count"
    )
    last_job_count_df = last_job_count_df.withColumnRenamed(
        "rolling_average", "last_rolling_average"
    )
    last_job_count_df = last_job_count_df.select(
        "locationid", "last_job_count", "last_rolling_average"
    )

    for_extrapolation = for_extrapolation.join(grouped_df, "locationid", "left")
    for_extrapolation = for_extrapolation.join(
        average_df, ["primary_service_type", "unix_time"], "left"
    )
    for_extrapolation = for_extrapolation.join(first_job_count_df, "locationid", "left")
    for_extrapolation = for_extrapolation.join(last_job_count_df, "locationid", "left")

    df_with_new_columns = for_extrapolation.withColumn(
        "forward_extrapolation_ratio", F.lit(None).cast(DoubleType())
    )
    df_with_new_columns = df_with_new_columns.withColumn(
        "backward_extrapolation_ratio", F.lit(None).cast(DoubleType())
    )
    df_with_new_columns = df_with_new_columns.withColumn(
        "extrapolation_model", F.lit(None).cast(DoubleType())
    )

    df_with_forward_rows = df_with_new_columns.where(
        df_with_new_columns.unix_time > df_with_new_columns.u_time_max
    )
    df_with_backward_rows = df_with_new_columns.where(
        df_with_new_columns.unix_time < df_with_new_columns.u_time_min
    )

    df_with_forward_rows = df_with_forward_rows.withColumn(
        "forward_extrapolation_ratio",
        (
            1
            + (F.col("last_rolling_average") - F.col("rolling_average"))
            / F.col("last_rolling_average")
        ),
    )
    df_with_forward_rows = df_with_forward_rows.withColumn(
        "extrapolation_model",
        (F.col("last_job_count") * F.col("forward_extrapolation_ratio")),
    )

    df_with_backward_rows = df_with_backward_rows.withColumn(
        "backward_extrapolation_ratio",
        (
            1
            + (F.col("first_rolling_average") - F.col("rolling_average"))
            / F.col("first_rolling_average")
        ),
    )
    df_with_backward_rows = df_with_backward_rows.withColumn(
        "extrapolation_model",
        (F.col("first_job_count") * F.col("backward_extrapolation_ratio")),
    )

    df_with_extrapolation_models = df_with_forward_rows.unionByName(
        df_with_backward_rows
    )

    df_with_extrapolation_models = df_with_extrapolation_models.select(
        "locationid", "snapshot_date", "extrapolation_model"
    )

    df = df.join(
        df_with_extrapolation_models, ["locationid", "snapshot_date"], "leftouter"
    )

    df = df.join(average_df, ["primary_service_type", "unix_time"], "left")

    df = df.drop("unix_time")

    return df


def convert_date_to_unix_timestamp(
    df: pyspark.sql.DataFrame, date_col: str, date_format: str, new_col_name: str
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        new_col_name, F.unix_timestamp(F.col(date_col), format=date_format)
    )

    return df


def rolling_total(col_to_average: str, unix_date_col: str, number_of_days: int):
    return F.sum(col_to_average).over(
        rolling_average_time_period(unix_date_col, number_of_days)
    )


def rolling_average_time_period(unix_date_col: str, number_of_days: int):
    return (
        Window.partitionBy("primary_service_type")
        .orderBy(F.col(unix_date_col).cast("long"))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )


def convert_days_to_unix_time(days: int):
    return days * 86400


def create_rolling_average_column(
    df: pyspark.sql.DataFrame, number_of_days: int
) -> pyspark.sql.DataFrame:

    df = df.withColumn(
        "rolling_total_jobs",
        rolling_total("total_job_count", "unix_time", number_of_days),
    )

    df = df.withColumn(
        "rolling_total_count",
        rolling_total("count", "unix_time", number_of_days),
    )

    df = df.withColumn(
        "rolling_average", F.col("rolling_total_jobs") / F.col("rolling_total_count")
    )

    return df
