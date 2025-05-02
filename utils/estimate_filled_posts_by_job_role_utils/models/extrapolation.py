from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.estimate_filled_posts.models.extrapolation import (
    define_window_specs,
    calculate_first_and_final_submission_dates,
)
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


def extrapolate_job_role_ratios(df: DataFrame) -> DataFrame:
    """
    Extrapolate job role ratios by copying the first and last known values for each location.

    Logic:
    - For each location_id:
      - Copy the first known job role ratios backward to all earlier dates.
      - Copy the last known job role ratios forward to all later dates.
      - Do nothing in between first and last known points, assume interpolation has already been performed.

    Args:
        df (DataFrame): A dataframe with job role ratios.

    Returns:
        DataFrame: The input dataframe with an additional column of extrapolated job role ratios.
    """

    window_spec_all_rows, window_spec_lagged = define_window_specs()

    df = calculate_first_and_final_submission_dates(
        df, IndCqc.ascwds_job_role_ratios_filtered, window_spec_all_rows
    )

    job_role_ratios_with_nulls = IndCqc.ascwds_job_role_ratios_filtered
    time_col = IndCqc.unix_time

    temp_first_known_value = "first_known_value"
    temp_last_known_value = "temp_last_known_value"
    df = get_selected_value(
        df,
        window_spec_all_rows,
        job_role_ratios_with_nulls,
        job_role_ratios_with_nulls,
        temp_first_known_value,
        "first",
    )
    df = get_selected_value(
        df,
        window_spec_all_rows,
        job_role_ratios_with_nulls,
        job_role_ratios_with_nulls,
        temp_last_known_value,
        "last",
    )

    df = df.withColumn(
        IndCqc.ascwds_job_role_ratios_extrapolated,
        F.when(
            F.col(job_role_ratios_with_nulls).isNotNull(),
            F.col(job_role_ratios_with_nulls),
        )
        .when(
            F.col(time_col) <= F.col(IndCqc.first_submission_time),
            F.col(temp_first_known_value),
        )
        .when(
            F.col(time_col) >= F.col(IndCqc.final_submission_time),
            F.col(temp_last_known_value),
        ),
    )

    columns_to_drop = [
        IndCqc.first_submission_time,
        IndCqc.final_submission_time,
        temp_first_known_value,
        temp_last_known_value,
    ]
    df = df.drop(*columns_to_drop)

    return df
