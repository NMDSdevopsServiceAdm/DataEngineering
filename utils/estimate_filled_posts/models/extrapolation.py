from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_extrapolation(df: DataFrame) -> DataFrame:
    filtered_df = filter_to_locations_who_have_a_filled_posts_at_some_point(df)

    filtered_with_first_and_last_submitted_data_df = (
        add_filled_posts_and_rolling_average_for_first_and_last_submission(filtered_df)
    )

    df_with_extrapolated_values = add_extrapolated_values(
        df, filtered_with_first_and_last_submitted_data_df
    )

    return df_with_extrapolated_values


def filter_to_locations_who_have_a_filled_posts_at_some_point(
    df: DataFrame,
) -> DataFrame:
    max_filled_posts_per_location_df = calculate_max_filled_posts_for_each_location(df)
    max_filled_posts_per_location_df = filter_to_known_values_only(
        max_filled_posts_per_location_df
    )

    return left_join_on_locationid(max_filled_posts_per_location_df, df)


def calculate_max_filled_posts_for_each_location(df: DataFrame) -> DataFrame:
    max_filled_posts_for_each_location_df = df.groupBy(IndCqc.location_id).agg(
        F.max(IndCqc.ascwds_filled_posts_dedup_clean).alias(IndCqc.max_filled_posts)
    )
    return max_filled_posts_for_each_location_df


def filter_to_known_values_only(df: DataFrame) -> DataFrame:
    return df.where(F.col(IndCqc.max_filled_posts) > 0.0)


def add_filled_posts_and_rolling_average_for_first_and_last_submission(
    df: DataFrame,
    model_column_name: str,
    new_model_column_name: str,
) -> DataFrame:
    df = add_first_and_last_submission_date_cols(df)

    df = add_filled_posts_and_modelled_value_for_specific_time_period(
        df,
        IndCqc.first_submission_time,
        IndCqc.first_filled_posts,
        model_column_name,
        new_model_column_name,
    )

    df = add_filled_posts_and_modelled_value_for_specific_time_period(
        df,
        IndCqc.last_submission_time,
        IndCqc.last_filled_posts,
        model_column_name,
        new_model_column_name,
    )

    return df


def add_first_and_last_submission_date_cols(df: DataFrame) -> DataFrame:
    populated_filled_posts_df = df.where(
        F.col(IndCqc.ascwds_filled_posts_dedup_clean).isNotNull()
    )

    first_and_last_submission_date_df = populated_filled_posts_df.groupBy(
        IndCqc.location_id
    ).agg(
        F.min(IndCqc.unix_time).cast("integer").alias(IndCqc.first_submission_time),
        F.max(IndCqc.unix_time).cast("integer").alias(IndCqc.last_submission_time),
    )

    return left_join_on_locationid(df, first_and_last_submission_date_df)


def add_filled_posts_and_modelled_value_for_specific_time_period(
    df: DataFrame,
    unix_time_period: str,
    new_filled_posts_col_name: str,
    model_column_name: str,
    new_model_column_name: str,
) -> DataFrame:
    unix_time_df = df.where(F.col(unix_time_period) == F.col(IndCqc.unix_time))
    unix_time_df = unix_time_df.withColumnRenamed(
        IndCqc.ascwds_filled_posts_dedup_clean, new_filled_posts_col_name
    )
    unix_time_df = unix_time_df.withColumnRenamed(
        model_column_name, new_model_column_name
    )
    unix_time_df = unix_time_df.select(
        IndCqc.location_id, new_filled_posts_col_name, new_model_column_name
    )

    return left_join_on_locationid(df, unix_time_df)


def add_extrapolated_values(df: DataFrame, extrapolation_df: DataFrame) -> DataFrame:
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

    df = df.join(
        df_with_extrapolation_models,
        [IndCqc.location_id, IndCqc.cqc_location_import_date],
        "leftouter",
    )
    return df


def create_extrapolation_ratio_column(df: DataFrame) -> DataFrame:  # TODO: Refactor
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


def create_extrapolation_model_column(df: DataFrame) -> DataFrame:
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

    df = df.select(
        IndCqc.location_id, IndCqc.cqc_location_import_date, IndCqc.extrapolation_model
    )

    return df


def left_join_on_locationid(main_df: DataFrame, data_to_add_df: DataFrame) -> DataFrame:
    return main_df.join(data_to_add_df, IndCqc.location_id, "left")
