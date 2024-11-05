from pyspark.sql import DataFrame, functions as F, Window
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


def model_interpolation(
    df: DataFrame,
    column_with_null_values: str,
) -> DataFrame:
    """
    Perform interpolation on a column with null values and adds as a new column called 'interpolation_model'.

    This function uses the extrapolation_forwards values as a trend line to guide interpolated predictions
    between two non-null values and inputs the interpolated results into a new column called 'interpolation_model'.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be interpolated.

    Returns:
        DataFrame: The DataFrame with the interpolated values in the 'interpolation_model' column.
    """
    window_spec_backwards, window_spec_forwards = define_window_specs()

    df = calculate_residuals(df, column_with_null_values, window_spec_forwards)

    df = calculate_proportion_of_time_between_submissions(
        df, column_with_null_values, window_spec_backwards, window_spec_forwards
    )

    df = df.withColumn(
        IndCqc.interpolation_model,
        F.col(IndCqc.extrapolation_forwards)
        + F.col(IndCqc.extrapolation_residual)
        * F.col(IndCqc.proportion_of_time_between_submissions),
    )

    return df


def define_window_specs() -> Tuple[Window, Window]:
    """
    Defines two window specifications, partitioned by 'location_id' and ordered by 'unix_time'.

    The first window specification ('window_spec_backwards') includes all rows up to the current row.
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


def calculate_residuals(
    df: DataFrame, column_with_null_values: str, window_spec_forward: Window
) -> DataFrame:
    """
    Calculate the residual between non-null values and the extrapolation_forwards value.

    This function computes the residuals between non-null values in a specified column and the forward extrapolated values.
    It creates a temporary column to store the difference between the non-null values and the extrapolated values, then calculates
    the first non-null residual over a specified window and assigns it to a new column.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values.
        window_spec_forward (Window): The window specification for getting the next residual value.

    Returns:
        DataFrame: The DataFrame with the calculated residuals in a new column.
    """
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
    """
    Calculates the proportion of time, based on unix_time of each row, between two non-null submission times.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values.
        window_spec_backwards (Window): The window specification for getting the unix_time of the previous non-null value.
        window_spec_forwards (Window): The window specification for getting the unix_time of the next non-null value.

    Returns:
        DataFrame: The DataFrame with the new column added.
    """
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
