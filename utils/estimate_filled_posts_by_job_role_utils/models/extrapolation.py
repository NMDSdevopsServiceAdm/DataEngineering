from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.estimate_filled_posts.models.extrapolation import (
    define_window_specs,
    calculate_first_and_final_submission_dates,
)


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

    first_known_value = F.first(
        F.col(job_role_ratios_with_nulls), ignorenulls=True
    ).over(window_spec_all_rows)
    last_known_value = F.last(F.col(job_role_ratios_with_nulls), ignorenulls=True).over(
        window_spec_all_rows
    )
    df = df.withColumn(
        IndCqc.ascwds_job_role_ratios_extrapolated,
        F.when(
            F.col(job_role_ratios_with_nulls).isNotNull(),
            F.col(job_role_ratios_with_nulls),
        )
        .when(F.col(time_col) <= F.col(IndCqc.first_submission_time), first_known_value)
        .when(F.col(time_col) >= F.col(IndCqc.final_submission_time), last_known_value),
    )

    """
    input_col = IndCqc.ascwds_job_role_ratios_filtered
    output_col = IndCqc.ascwds_job_role_ratios_extrapolated
    group_col = IndCqc.location_id


    base_window = Window.partitionBy(group_col).orderBy(time_col)
    full_window = base_window.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    """

    return df
