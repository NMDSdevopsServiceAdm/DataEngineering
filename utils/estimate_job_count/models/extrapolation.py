import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import pyspark.sql

from utils.estimate_job_count.column_names import (
    LOCATION_ID,
    SNAPSHOT_DATE,
    UNIX_TIME,
    JOB_COUNT,
    ESTIMATE_JOB_COUNT,
    ROLLING_AVERAGE_MODEL,
)
from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)

MAX_JOB_COUNT = "max_job_count"
FIRST_SUBMISSION_TIME = "first_submission_time"
FIRST_ROLLING_AVERAGE = "first_rolling_average"
FIRST_JOB_COUNT = "first_job_count"
LAST_SUBMISSION_TIME = "last_submission_time"
LAST_ROLLING_AVERAGE = "last_rolling_average"
LAST_JOB_COUNT = "last_job_count"
EXTRAPOLATION_RATIO = "extrapolation_ratio"
EXTRAPOLATION_MODEL = "extrapolation_model"


def model_extrapolation(df: DataFrame) -> DataFrame:
    for_extrapolation = filter_to_locations_who_have_a_job_count_at_some_point(df)

    for_extrapolation = add_job_count_and_rolling_average_for_first_and_last_submission(
        for_extrapolation
    )

    df = add_extrapolated_values(df, for_extrapolation)

    df = df.withColumn(
        ESTIMATE_JOB_COUNT,
        F.when(
            F.col(ESTIMATE_JOB_COUNT).isNotNull(), F.col(ESTIMATE_JOB_COUNT)
        ).otherwise(F.col(EXTRAPOLATION_MODEL)),
    )
    df = update_dataframe_with_identifying_rule(
        df, EXTRAPOLATION_MODEL, ESTIMATE_JOB_COUNT
    )

    return df


def filter_to_locations_who_have_a_job_count_at_some_point(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    max_job_count_per_location_df = calculate_max_job_count_for_each_location(df)
    max_job_count_per_location_df = filter_to_known_values_only(
        max_job_count_per_location_df
    )

    return left_join_on_locationid(max_job_count_per_location_df, df)


def calculate_max_job_count_for_each_location(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    return df.groupBy(LOCATION_ID).agg(F.max(JOB_COUNT).alias(MAX_JOB_COUNT))


def filter_to_known_values_only(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    return df.where(F.col(MAX_JOB_COUNT) > 0.0)


def add_job_count_and_rolling_average_for_first_and_last_submission(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = add_first_and_last_submission_date_cols(df)

    df = add_job_count_and_rolling_average_for_specific_time_period(
        df,
        FIRST_SUBMISSION_TIME,
        FIRST_JOB_COUNT,
        FIRST_ROLLING_AVERAGE,
    )

    df = add_job_count_and_rolling_average_for_specific_time_period(
        df,
        LAST_SUBMISSION_TIME,
        LAST_JOB_COUNT,
        LAST_ROLLING_AVERAGE,
    )

    return df


def add_first_and_last_submission_date_cols(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    populated_job_count_df = df.where(F.col(JOB_COUNT).isNotNull())

    first_and_last_submission_date_df = populated_job_count_df.groupBy(LOCATION_ID).agg(
        F.min(UNIX_TIME).cast("integer").alias(FIRST_SUBMISSION_TIME),
        F.max(UNIX_TIME).cast("integer").alias(LAST_SUBMISSION_TIME),
    )

    return left_join_on_locationid(df, first_and_last_submission_date_df)


def add_job_count_and_rolling_average_for_specific_time_period(
    df: pyspark.sql.DataFrame,
    unix_time_period: str,
    new_job_count_col_name: str,
    new_rolling_average_col_name: str,
) -> pyspark.sql.DataFrame:
    unix_time_df = df.where(F.col(unix_time_period) == F.col(UNIX_TIME))
    unix_time_df = unix_time_df.withColumnRenamed(JOB_COUNT, new_job_count_col_name)
    unix_time_df = unix_time_df.withColumnRenamed(
        ROLLING_AVERAGE_MODEL, new_rolling_average_col_name
    )
    unix_time_df = unix_time_df.select(
        LOCATION_ID, new_job_count_col_name, new_rolling_average_col_name
    )

    return left_join_on_locationid(df, unix_time_df)


def add_extrapolated_values(
    df: pyspark.sql.DataFrame, extrapolation_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    extrapolation_ratios_before_first_submission_df = (
        calculate_extrapolation_ratios_before_first_submission(extrapolation_df)
    )
    extrapolation_ratios_after_last_submission_df = (
        calculate_extrapolation_ratios_after_last_submission(extrapolation_df)
    )

    df_with_extrapolation_models = (
        extrapolation_ratios_before_first_submission_df.unionByName(
            extrapolation_ratios_after_last_submission_df
        )
    )

    df_with_extrapolation_models = df_with_extrapolation_models.select(
        LOCATION_ID, SNAPSHOT_DATE, EXTRAPOLATION_MODEL
    )

    return df.join(
        df_with_extrapolation_models, [LOCATION_ID, SNAPSHOT_DATE], "leftouter"
    )


def calculate_extrapolation_ratios_before_first_submission(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    relevant_data_df = df.where(F.col(UNIX_TIME) < F.col(FIRST_SUBMISSION_TIME))
    relevant_data_df = calculate_extrapolation_ratio(
        relevant_data_df, FIRST_ROLLING_AVERAGE
    )
    return calculate_extrapolated_value(relevant_data_df, FIRST_JOB_COUNT)


def calculate_extrapolation_ratios_after_last_submission(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    relevant_data_df = df.where(F.col(UNIX_TIME) > F.col(LAST_SUBMISSION_TIME))
    relevant_data_df = calculate_extrapolation_ratio(
        relevant_data_df, LAST_ROLLING_AVERAGE
    )
    return calculate_extrapolated_value(relevant_data_df, LAST_JOB_COUNT)


def calculate_extrapolation_ratio(
    df: pyspark.sql.DataFrame, first_or_last_rolling_avg: str
) -> pyspark.sql.DataFrame:
    return df.withColumn(
        EXTRAPOLATION_RATIO,
        (
            1
            + (F.col(ROLLING_AVERAGE_MODEL) - F.col(first_or_last_rolling_avg))
            / F.col(first_or_last_rolling_avg)
        ),
    )


def calculate_extrapolated_value(
    df: pyspark.sql.DataFrame, first_or_last_job_count: str
) -> pyspark.sql.DataFrame:
    return df.withColumn(
        EXTRAPOLATION_MODEL,
        (F.col(first_or_last_job_count) * F.col(EXTRAPOLATION_RATIO)),
    )


def left_join_on_locationid(
    main_df: pyspark.sql.DataFrame, data_to_add_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    return main_df.join(data_to_add_df, LOCATION_ID, "left")
