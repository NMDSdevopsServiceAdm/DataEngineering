import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import pyspark.sql


from utils.prepare_locations_utils.job_calculator.job_calculator import (
    update_dataframe_with_identifying_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_extrapolation(df: DataFrame) -> DataFrame:
    filtered_df = filter_to_locations_who_have_a_job_count_at_some_point(df)

    filtered_with_first_and_last_submitted_data_df = (
        add_job_count_and_rolling_average_for_first_and_last_submission(filtered_df)
    )

    df_with_extrapolated_values = add_extrapolated_values(
        df, filtered_with_first_and_last_submitted_data_df
    )

    df_with_extrapolated_values = df_with_extrapolated_values.withColumn(
        IndCqc.estimate_filled_posts,
        F.when(
            F.col(IndCqc.estimate_filled_posts).isNotNull(),
            F.col(IndCqc.estimate_filled_posts),
        ).otherwise(F.col(IndCqc.extrapolation_model)),
    )
    df_with_extrapolated_values = update_dataframe_with_identifying_rule(
        df_with_extrapolated_values,
        IndCqc.extrapolation_model,
        IndCqc.estimate_filled_posts,
    )
    return df_with_extrapolated_values


def filter_to_locations_who_have_a_job_count_at_some_point(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    max_filled_posts_per_location_df = calculate_max_filled_posts_for_each_location(df)
    max_filled_posts_per_location_df = filter_to_known_values_only(
        max_filled_posts_per_location_df
    )

    return left_join_on_locationid(max_filled_posts_per_location_df, df)


def calculate_max_filled_posts_for_each_location(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    max_filled_posts_for_each_location_df = df.groupBy(IndCqc.location_id).agg(
        F.max(IndCqc.ascwds_filled_posts_dedup_clean).alias(IndCqc.max_filled_posts)
    )
    return max_filled_posts_for_each_location_df


def filter_to_known_values_only(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    return df.where(F.col(IndCqc.max_filled_posts) > 0.0)


def add_job_count_and_rolling_average_for_first_and_last_submission(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = add_first_and_last_submission_date_cols(df)

    df = add_job_count_and_rolling_average_for_specific_time_period(
        df,
        IndCqc.first_submission_time,
        IndCqc.first_filled_posts,
        IndCqc.first_rolling_average,
    )

    df = add_job_count_and_rolling_average_for_specific_time_period(
        df,
        IndCqc.last_submission_time,
        IndCqc.last_filled_posts,
        IndCqc.last_rolling_average,
    )

    return df


def add_first_and_last_submission_date_cols(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    populated_job_count_df = df.where(
        F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull()
    )

    first_and_last_submission_date_df = populated_job_count_df.groupBy(
        IndCqc.location_id
    ).agg(
        F.min(IndCqc.unix_time).cast("integer").alias(IndCqc.first_submission_time),
        F.max(IndCqc.unix_time).cast("integer").alias(IndCqc.last_submission_time),
    )

    return left_join_on_locationid(df, first_and_last_submission_date_df)


def add_job_count_and_rolling_average_for_specific_time_period(
    df: pyspark.sql.DataFrame,
    unix_time_period: str,
    new_job_count_col_name: str,
    new_rolling_average_col_name: str,
) -> pyspark.sql.DataFrame:
    unix_time_df = df.where(F.col(unix_time_period) == F.col(IndCqc.unix_time))
    unix_time_df = unix_time_df.withColumnRenamed(
        IndCqc.ascwds_filled_posts_dedup_clean, new_job_count_col_name
    )
    unix_time_df = unix_time_df.withColumnRenamed(
        IndCqc.rolling_average_model, new_rolling_average_col_name
    )
    unix_time_df = unix_time_df.select(
        IndCqc.location_id, new_job_count_col_name, new_rolling_average_col_name
    )

    return left_join_on_locationid(df, unix_time_df)


def add_extrapolated_values(
    df: pyspark.sql.DataFrame, extrapolation_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    df_with_extrapolation_models = extrapolation_df.where(
        (F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time))
        | (F.col(IndCqc.unix_time) > F.col(IndCqc.last_submission_time))
    )

    df_with_extrapolation_models = create_extrapolation_ratio_column(
        df_with_extrapolation_models
    )

    df_with_extrapolation_models = create_extrapolation_model_column(
        df_with_extrapolation_models
    )

    return df.join(
        df_with_extrapolation_models,
        [IndCqc.location_id, IndCqc.cqc_location_import_date],
        "leftouter",
    )


def create_extrapolation_ratio_column(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df_with_extrapolation_ratio_column = df.withColumn(
        IndCqc.extrapolation_ratio,
        F.when(
            (F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time)),
            (
                1
                + (
                    F.col(IndCqc.rolling_average_model)
                    - F.col(IndCqc.first_rolling_average)
                )
                / F.col(IndCqc.first_rolling_average)
            ),
        ).when(
            (F.col(IndCqc.unix_time) > F.col(IndCqc.last_submission_time)),
            (
                1
                + (
                    F.col(IndCqc.rolling_average_model)
                    - F.col(IndCqc.last_rolling_average)
                )
                / F.col(IndCqc.last_rolling_average)
            ),
        ),
    )
    return df_with_extrapolation_ratio_column


def create_extrapolation_model_column(
    df: pyspark.sql.DataFrame,
) -> pyspark.sql.DataFrame:
    df = df.withColumn(
        IndCqc.extrapolation_model,
        F.when(
            (F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time)),
            (F.col(IndCqc.first_filled_posts) * F.col(IndCqc.extrapolation_ratio)),
        ).when(
            (F.col(IndCqc.unix_time) > F.col(IndCqc.last_submission_time)),
            (F.col(IndCqc.last_filled_posts) * F.col(IndCqc.extrapolation_ratio)),
        ),
    )

    return df.select(
        IndCqc.location_id, IndCqc.cqc_location_import_date, IndCqc.extrapolation_model
    )


def left_join_on_locationid(
    main_df: pyspark.sql.DataFrame, data_to_add_df: pyspark.sql.DataFrame
) -> pyspark.sql.DataFrame:
    return main_df.join(data_to_add_df, IndCqc.location_id, "left")
