from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def extrapolate_job_role_ratios(df: DataFrame) -> DataFrame:
    """
    Extrapolate job role ratios by copying the first and last known values for each location.

    Logic:
    - For each location_id:
      - Copy the first known job role ratios backward to all earlier dates.
      - Copy the last known job role ratios forward to all later dates.
      - In between first and last known points, assume interpolation has already been performed.

    Input:
        - IndCqc.ascwds_job_role_ratios_filtered (MapType): Map of job role ratios per location and time. May contain nulls.

    Output:
        - IndCqc.ascwds_job_role_ratios_extrapolated (MapType): Fully populated by extrapolating known values forward and backward.
    """
    input_col = IndCqc.ascwds_job_role_ratios_filtered
    output_col = IndCqc.ascwds_job_role_ratios_extrapolated
    group_col = IndCqc.location_id
    time_col = IndCqc.unix_time

    base_window = Window.partitionBy(group_col).orderBy(time_col)
    full_window = base_window.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    past_window = base_window.rowsBetween(Window.unboundedPreceding, 0)

    first_known_value = F.first(F.col(input_col), ignorenulls=True).over(full_window)
    first_known_time = F.first(
        F.when(F.col(input_col).isNotNull(), F.col(time_col)), ignorenulls=True
    ).over(full_window)
    last_known_value = F.last(F.col(input_col), ignorenulls=True).over(full_window)
    last_known_time = F.last(
        F.when(F.col(input_col).isNotNull(), F.col(time_col)), ignorenulls=True
    ).over(full_window)
    forward_filled_value = F.last(F.col(input_col), ignorenulls=True).over(past_window)

    extrapolated_col = (
        F.when(F.col(input_col).isNotNull(), F.col(input_col))
        .when(F.col(time_col) <= first_known_time, first_known_value)
        .when(F.col(time_col) >= last_known_time, last_known_value)
        .otherwise(forward_filled_value)
    )

    return df.withColumn(output_col, extrapolated_col)
