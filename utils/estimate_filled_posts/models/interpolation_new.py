from pyspark.sql import DataFrame, functions as F, Window
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


def model_interpolation(
    df: DataFrame,
    column_with_null_values: str,
    interpolated_column_name: str,
) -> DataFrame:
    window_spec_backwards, window_spec_forwards = define_window_specs()

    df = calculate_residual_between_non_null_value_and_extrapolation_forwards(
        df, column_with_null_values, window_spec_forwards
    )

    df = calculate_proportion_of_time_between_submissions(
        df, column_with_null_values, window_spec_backwards, window_spec_forwards
    )

    df = df.withColumn(
        interpolated_column_name,
        F.col(IndCqc.extrapolation_forwards)
        + F.col(IndCqc.extrapolation_residual)
        * F.col(IndCqc.proportion_of_time_between_submissions),
    )

    return df


def define_window_specs() -> Tuple[Window, Window]:
    """
    Defines two window specifications, partitioned by 'location_id' and ordered by 'unix_time'.

    The first window specification ('window_spec_all_rows') includes all rows in the partition.
    The second window specification ('window_spec_forward') includes all rows from the current row onwards.

    Returns:
        Tuple[Window, Window]: A tuple containing the two window specifications.
    """
    window_spec = Window.partitionBy(IndCqc.location_id).orderBy(IndCqc.unix_time)

    window_spec_backwards = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    window_spec_forward = window_spec.rowsBetween(
        Window.currentRow, Window.unboundedFollowing
    )

    return window_spec_backwards, window_spec_forward


def calculate_residual_between_non_null_value_and_extrapolation_forwards(
    df: DataFrame, column_with_null_values: str, window_spec_forward: Window
) -> DataFrame:
    temp_col: str = "temp_col"
    df = df.withColumn(
        temp_col,
        F.when(
            F.col(column_with_null_values).isNotNull()
            & F.col(IndCqc.extrapolation_forwards).isNotNull(),
            F.col(column_with_null_values) - F.col(IndCqc.extrapolation_forwards),
        ),
    )

    df = df.withColumn(
        IndCqc.extrapolation_residual,
        F.when(
            F.col(IndCqc.extrapolation_forwards).isNotNull(),
            F.first(
                F.col(temp_col),
                ignorenulls=True,
            ).over(window_spec_forward),
        ),
    ).drop(temp_col)

    return df


def calculate_proportion_of_time_between_submissions(
    df: DataFrame,
    column_with_null_values: str,
    window_spec_backwards: Window,
    window_spec_forwards: Window,
) -> DataFrame:
    df = get_selected_value(
        df,
        window_spec_backwards,
        column_with_null_values,
        IndCqc.unix_time,
        IndCqc.previous_submission_time,
        "last",
    )
    df = get_selected_value(
        df,
        window_spec_forwards,
        column_with_null_values,
        IndCqc.unix_time,
        IndCqc.next_submission_time,
        "first",
    )

    df = df.withColumn(
        IndCqc.proportion_of_time_between_submissions,
        F.when(
            (F.col(IndCqc.previous_submission_time) < F.col(IndCqc.unix_time))
            & (F.col(IndCqc.next_submission_time) > F.col(IndCqc.unix_time)),
            (F.col(IndCqc.unix_time) - F.col(IndCqc.previous_submission_time))
            / (
                F.col(IndCqc.next_submission_time)
                - F.col(IndCqc.previous_submission_time)
            ),
        ),
    ).drop(IndCqc.previous_submission_time, IndCqc.next_submission_time)

    return df
