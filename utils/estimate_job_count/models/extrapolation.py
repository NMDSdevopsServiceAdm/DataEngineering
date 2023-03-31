import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
import pyspark.sql

from utils.estimate_job_count.column_names import ESTIMATE_JOB_COUNT
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)


def model_extrapolation(df: DataFrame) -> DataFrame:

    # for_extrapolation = df.select(
    #     "locationid", "snapshot_date", "unix_time", "job_count", "primary_service_type", "rolling_average_model"
    # )

    for_extrapolation = filter_to_locations_who_have_a_job_count_at_some_point(df)

    first_and_last_submission_df = add_first_and_last_submission_date_cols(
        for_extrapolation
    )

    for_extrapolation = for_extrapolation.join(
        first_and_last_submission_df, "locationid", "left"
    )

    first_job_count_df = for_extrapolation.where(
        for_extrapolation.first_submission_time == for_extrapolation.unix_time
    )
    first_job_count_df = first_job_count_df.withColumnRenamed(
        "job_count", "first_job_count"
    )
    first_job_count_df = first_job_count_df.withColumnRenamed(
        "rolling_average_model", "first_rolling_average"
    )
    first_job_count_df = first_job_count_df.select(
        "locationid", "first_job_count", "first_rolling_average"
    )

    last_job_count_df = for_extrapolation.where(
        for_extrapolation.last_submission_time == for_extrapolation.unix_time
    )
    last_job_count_df = last_job_count_df.withColumnRenamed(
        "job_count", "last_job_count"
    )
    last_job_count_df = last_job_count_df.withColumnRenamed(
        "rolling_average_model", "last_rolling_average"
    )
    last_job_count_df = last_job_count_df.select(
        "locationid", "last_job_count", "last_rolling_average"
    )

    # for_extrapolation = for_extrapolation.join(average_df, ["primary_service_type", "unix_time"], "left")
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
        df_with_new_columns.unix_time > df_with_new_columns.last_submission_time
    )
    df_with_backward_rows = df_with_new_columns.where(
        df_with_new_columns.unix_time < df_with_new_columns.first_submission_time
    )

    df_with_forward_rows = df_with_forward_rows.withColumn(
        "forward_extrapolation_ratio",
        (
            1
            + (F.col("rolling_average_model") - F.col("last_rolling_average"))
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
            + (F.col("rolling_average_model") - F.col("first_rolling_average"))
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

    # df = df.join(average_df, ["primary_service_type", "unix_time"], "left")

    df = df.drop("unix_time")

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
        ).otherwise(F.col("extrapolation_model")),
    )
    df = update_dataframe_with_identifying_rule(
        df, "extrapolation_model", ESTIMATE_JOB_COUNT
    )
    df.show()

    return df


def filter_to_locations_who_have_a_job_count_at_some_point(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:

    max_job_count_per_location_df = df.groupBy("locationid").agg(
        F.max("job_count").alias("max_job_count")
    )
    max_job_count_per_location_df = max_job_count_per_location_df.where(
        F.col("max_job_count") > 0.0
    )

    return max_job_count_per_location_df.join(df, "locationid", "left")


def add_first_and_last_submission_date_cols(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    populated_job_count_df = df.where(F.col("job_count").isNotNull())

    return populated_job_count_df.groupBy("locationid").agg(
        F.min("unix_time").cast("integer").alias("first_submission_time"),
        F.max("unix_time").cast("integer").alias("last_submission_time"),
    )
