from pyspark.sql import DataFrame, functions as F, Window
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


def model_extrapolation(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
) -> DataFrame:
    """
    Perform extrapolation on a column with null values using specified models.

    This function calculates the first and final submission dates, performs forward and backward extrapolation,
    and combines the extrapolated results into a new column.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.

    Returns:
        DataFrame: The DataFrame with the extrapolated values in the 'extrapolation_model' column.
    """
    window_spec_all_rows, window_spec_lagged = define_window_specs()

    df = calculate_first_and_final_submission_dates(
        df, column_with_null_values, window_spec_all_rows
    )
    df = extrapolation_forwards(
        df, column_with_null_values, model_to_extrapolate_from, window_spec_lagged
    )
    df = extrapolation_backwards(
        df, column_with_null_values, model_to_extrapolate_from, window_spec_all_rows
    )
    df = combine_extrapolation(df)
    df = df.drop(IndCqc.first_submission_time, IndCqc.final_submission_time)

    return df


def define_window_specs() -> Tuple[Window, Window]:
    """
    Defines two window specifications, partitioned by 'location_id' and ordered by 'unix_time'.

    The first window specification ('window_spec_all_rows') includes all rows in the partition.
    The second window specification ('window_spec_lagged') includes all rows from the start of the partition up to the
    current row, excluding the current row.

    Returns:
        Tuple[Window, Window]: A tuple containing the two window specifications.
    """
    window_spec = Window.partitionBy(IndCqc.location_id).orderBy(IndCqc.unix_time)

    window_spec_all_rows = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    window_spec_lagged = window_spec.rowsBetween(Window.unboundedPreceding, -1)

    return window_spec_all_rows, window_spec_lagged


def calculate_first_and_final_submission_dates(
    df: DataFrame, column_with_null_values: str, window_spec: Window
) -> DataFrame:
    """
    Calculates the first and final submission dates based on the '<column_with_null_values>' column.

    Calculates the first and final submission dates based on the '<column_with_null_values>' column
    and adds them as new columns 'first_submission_time' and 'final_submission_time'.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: The DataFrame with the added 'first_submission_time' and 'final_submission_time' columns.
    """
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        IndCqc.unix_time,
        IndCqc.first_submission_time,
        "first",
    )
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        IndCqc.unix_time,
        IndCqc.final_submission_time,
        "last",
    )
    return df


def extrapolation_forwards(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    window_spec: Window,
) -> DataFrame:
    """
    Calculates the forward extrapolation and adds it as a new column 'extrapolation_forwards'.

    Calculates the forward extrapolation based on the last known non-null value and the rate of change of the selected model value, and adds it as a new column 'extrapolation_forwards'.

    Args:
        df (DataFrame): A dataframe with a column to extrapolate forwards.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The model used for extrapolation.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: A dataframe with a new column containing forward extrapolated values.
    """
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        column_with_null_values,
        IndCqc.previous_non_null_value,
        "last",
    )
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        model_to_extrapolate_from,
        IndCqc.previous_model_value,
        "last",
    )

    df = df.withColumn(
        IndCqc.extrapolation_forwards,
        F.col(IndCqc.previous_non_null_value)
        * F.col(model_to_extrapolate_from)
        / F.col(IndCqc.previous_model_value),
    )

    df = df.drop(IndCqc.previous_non_null_value, IndCqc.previous_model_value)

    return df


def extrapolation_backwards(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    window_spec: Window,
) -> DataFrame:
    """
    Calculates the backward extrapolation and adds it as a new column 'extrapolation_backwards'.

    Calculates the backward extrapolation based on the first non null value and the first modelled value, and adds it as a new column 'extrapolation_backwards'.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The name of the column representing the model to extrapolate from.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: The DataFrame with the added 'extrapolation_backwards' column.
    """
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        column_with_null_values,
        IndCqc.first_non_null_value,
        "first",
    )

    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        model_to_extrapolate_from,
        IndCqc.first_model_value,
        "first",
    )

    df = df.withColumn(
        IndCqc.extrapolation_backwards,
        F.when(
            F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time),
            F.col(IndCqc.first_non_null_value)
            * (F.col(model_to_extrapolate_from) / F.col(IndCqc.first_model_value)),
        ),
    )
    df = df.drop(IndCqc.first_non_null_value, IndCqc.first_model_value)

    return df


def combine_extrapolation(df: DataFrame) -> DataFrame:
    """
    Combines forward and backward extrapolation values into a single column based on the specified model.

    This function creates a new column named 'extrapolation_model' which contains:
    - Forward extrapolation values if 'unix_time' is greater than the 'final_submission_time'.
    - Backward extrapolation values if 'unix_time' is less than the 'first_submission_time'.

    Args:
        df (DataFrame): The input DataFrame containing the columns 'unix_time', 'first_submission_time',
            'final_submission_time', 'extrapolation_forwards', and 'extrapolation_backwards'.

    Returns:
        DataFrame: The DataFrame with the added combined extrapolation column.
    """
    df = df.withColumn(
        IndCqc.extrapolation_model,
        F.when(
            F.col(IndCqc.unix_time) > F.col(IndCqc.final_submission_time),
            F.col(IndCqc.extrapolation_forwards),
        ).when(
            F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time),
            F.col(IndCqc.extrapolation_backwards),
        ),
    )
    return df


def extrapolate_job_role_ratios(df: DataFrame) -> DataFrame:
    """
    Extrapolate job role ratios by copying the first and last known values for each location.

    Logic:
    - For each location_id:
      - Copy the first known job role ratios backward to all earlier dates.
      - Copy the last known job role ratios forward to all later dates.
    - Assumes interpolation (filling between known points) has already been performed.

    Input:
        - ascwds_job_role_ratios_filtered (MapType): Map of job role ratios per location and time. May contain nulls.

    Output:
        - ascwds_job_role_ratios_extrapolated (MapType): Fully populated by extrapolating known values forward and backward.
    """
    input_col = IndCqc.ascwds_job_role_ratios_filtered
    output_col = IndCqc.ascwds_job_role_ratios_extrapolated
    group_col = IndCqc.location_id
    time_col = IndCqc.unix_time

    full_window = (
        Window.partitionBy(group_col)
        .orderBy(time_col)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    first_val = F.first(input_col, ignorenulls=True).over(full_window)

    extrapolated = F.when(F.col(input_col).isNotNull(), F.col(input_col)).otherwise(
        first_val
    )

    return df.withColumn(output_col, extrapolated)
