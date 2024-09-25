from pyspark.sql import DataFrame, functions as F, Window
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_extrapolation(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolated_column_name: str,
) -> DataFrame:
    """
    Perform extrapolation on a column with null values using specified models.

    This function calculates the first and last submission dates, performs forward and backward extrapolation,
    and combines the extrapolated results into a new column.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolated_column_name (str): The name of the new column to store extrapolated values.

    Returns:
        DataFrame: The DataFrame with the extrapolated values in the specified column.
    """
    window_spec_all_rows, window_spec_lagged = define_window_specs()

    df = calculate_first_and_last_submission_dates(
        df, column_with_null_values, window_spec_all_rows
    )
    df = extrapolation_forwards(
        df, column_with_null_values, model_to_extrapolate_from, window_spec_lagged
    )
    df = extrapolation_backwards(
        df, column_with_null_values, model_to_extrapolate_from, window_spec_all_rows
    )
    df = combine_extrapolation(df, extrapolated_column_name)
    df.sort(IndCqc.location_id, IndCqc.unix_time).show(50)

    return df


def define_window_specs() -> Tuple[Window, Window]:
    """
    Defines a window specification which is partitioned by 'location_id' and ordered by 'unix_time'.

    Returns:
        Window: The window specification partitioned by 'location_id' and ordered by 'unix_time'.
    """
    window_spec = Window.partitionBy(IndCqc.location_id).orderBy(IndCqc.unix_time)

    window_spec_all_rows = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    window_spec_lagged = window_spec.rowsBetween(Window.unboundedPreceding, -1)

    return window_spec_all_rows, window_spec_lagged


def calculate_first_and_last_submission_dates(
    df: DataFrame, column_with_null_values: str, window_spec: Window
) -> DataFrame:
    """
    Calculates the first and last submission dates based on the '<column_with_null_values>' column.

    Calculates the first and last submission dates based on the '<column_with_null_values>' column
    and adds them as new columns 'first_submission_time' and 'last_submission_time'.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: The DataFrame with the added 'first_submission_time' and 'last_submission_time' columns.
    """
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        IndCqc.unix_time,
        IndCqc.first_submission_time,
        "min",
    )
    df = get_selected_value(
        df,
        window_spec,
        column_with_null_values,
        IndCqc.unix_time,
        IndCqc.last_submission_time,
        "max",
    )
    return df


def extrapolation_forwards(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    window_spec: Window,
) -> DataFrame:
    """
    Adds a new column with forward extrapolated values.

    The function controls the stages within the forward extrapolation process.

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

    df = calculate_extrapolation_forwards(df, model_to_extrapolate_from)

    df = df.drop(IndCqc.previous_non_null_value, IndCqc.previous_model_value)

    return df


def calculate_extrapolation_forwards(
    df: DataFrame, model_to_extrapolate_from: str
) -> DataFrame:
    """
    Calculates the forward extrapolation based on a specified model and adds it as a new column 'extrapolation_forwards'.

    Args:
        df (DataFrame): The input DataFrame containing the columns 'previous_non_null_value', 'previous_model_value', and the specified model column.
        model_to_extrapolate_from (str): The name of the column representing the model to extrapolate from.

    Returns:
        DataFrame: The DataFrame with the added 'extrapolation_forwards' column.
    """
    df = df.withColumn(
        IndCqc.extrapolation_forwards,
        F.col(IndCqc.previous_non_null_value)
        * F.col(model_to_extrapolate_from)
        / F.col(IndCqc.previous_model_value),
    )
    return df


def extrapolation_backwards(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    window_spec: Window,
) -> DataFrame:
    """
    Calculates the backward extrapolation based on the first filled posts and the first modelled value, and adds it as a new column 'extrapolation_backwards'.

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


def combine_extrapolation(df: DataFrame, extrapolated_column_name: str) -> DataFrame:
    """
    Combines forward and backward extrapolation values into a single column based on the specified model.

    This function creates a new column named '<extrapolated_column_name>' which contains:
    - Forward extrapolation values if 'unix_time' is greater than the 'last_submission_time'.
    - Backward extrapolation values if 'unix_time' is less than the 'first_submission_time'.

    Args:
        df (DataFrame): The input DataFrame containing the columns 'unix_time', 'first_submission_time', 'last_submission_time', 'extrapolation_forwards', and 'extrapolation_backwards'.
        model_to_extrapolate_from (str): The name of the model column used for extrapolation.

    Returns:
        DataFrame: The DataFrame with the added combined extrapolation column.
    """
    df = df.withColumn(
        extrapolated_column_name,
        F.when(
            F.col(IndCqc.unix_time) > F.col(IndCqc.last_submission_time),
            F.col(IndCqc.extrapolation_forwards),
        ).when(
            F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time),
            F.col(IndCqc.extrapolation_backwards),
        ),
    )
    return df


def get_selected_value(
    df: DataFrame,
    window_spec: Window,
    column_with_null_values: str,
    column_with_data: str,
    new_column: str,
    selection: str,
) -> DataFrame:
    """
    Creates a new column with the selected value (min, max, first or last) from a given column.

    This function creates a new column by selecting a specified value over a given window on a given dataframe. It will
    only select values in the column with data that have null values in the original column.

    Args:
        df (DataFrame): A dataframe containing the supplied columns.
        window_spec (Window): A window describing how to prepare the dataframe.
        column_with_null_values (str): A column with missing data.
        column_with_data (str): A column with data for all the rows that column_with_null_values has data. This can be column_with_null_values itself.
        new_column (str): The name of the new column containing the resulting selected values.
        selection (str): One of 'min', 'max', 'first', or 'last'. This determines which pyspark window function will be used.

    Returns:
        DataFrame: A dataframe containing a new column with the selected value populated through each window.

    Raises:
        ValueError: If 'selection' is not one of the four permitted pyspark window functions.
    """
    if selection == "min":
        method: function = F.min
    elif selection == "max":
        method: function = F.max
    elif selection == "first":
        method: function = F.first
    elif selection == "last":
        method: function = F.last
    else:
        raise ValueError(
            f"Error: The selection parameter '{selection}' was not found. Please use 'min', 'max', 'first', or 'last'."
        )
    if (selection == "min") | (selection == "max"):
        df = df.withColumn(
            new_column,
            method(
                F.when(
                    F.col(column_with_null_values).isNotNull(),
                    F.col(column_with_data),
                )
            ).over(window_spec),
        )
    else:
        df = df.withColumn(
            new_column,
            method(
                F.when(
                    F.col(column_with_null_values).isNotNull(),
                    F.col(column_with_data),
                ),
                ignorenulls=True,
            ).over(window_spec),
        )
    return df
