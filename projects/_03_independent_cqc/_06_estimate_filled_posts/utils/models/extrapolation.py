from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from projects._03_independent_cqc.utils.utils.utils import get_selected_value
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def model_extrapolation(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolation_method: str,
) -> DataFrame:
    """
    Perform extrapolation on a column with null values using specified models.

    This function calculates the first and final submission dates, performs forward and backward extrapolation,
    and combines the extrapolated results into a new column.

    Values before the first known submission in 'column_with_null_values' and after the last known submission are
    extrapolated, either by nominal or ratio method as specified in 'extrapolation_method'.
    The ratio method is based on multiplying the known value by the rate of change of the '<model_column_name>'.
    The nominal method is based on adding/subtracting the nominal change of the '<model_column_name>' to the known value.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be extrapolated.
        model_to_extrapolate_from (str): The model used for extrapolation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        DataFrame: The DataFrame with the extrapolated values in the 'extrapolation_model' column.
    """
    window_spec_all_rows, window_spec_lagged = define_window_specs()

    df = calculate_first_and_final_submission_dates(
        df, column_with_null_values, window_spec_all_rows
    )
    df = extrapolation_forwards(
        df,
        column_with_null_values,
        model_to_extrapolate_from,
        window_spec_lagged,
        extrapolation_method,
    )
    df = extrapolation_backwards(
        df,
        column_with_null_values,
        model_to_extrapolate_from,
        window_spec_all_rows,
        extrapolation_method,
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
    extrapolation_method: str,
) -> DataFrame:
    """
    Calculates the forward extrapolation and adds it as a new column 'extrapolation_forwards'.

    This function fills null values occurring after the last known value by extrapolating forwards.

    The ratio method is based on multiplying the rate of change from the modelled value at the time
    of the last known non-null value and the modelled value after that point in time.

    The nominal method is based on adding/subtracting the difference between those two modelled values.

    Args:
        df (DataFrame): A dataframe with a column to extrapolate forwards.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The model used for extrapolation.
        window_spec (Window): The window specification to use for the calculation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        DataFrame: A dataframe with a new column containing forward extrapolated values.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
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

    if extrapolation_method == "ratio":
        df = df.withColumn(
            IndCqc.extrapolation_forwards,
            F.col(IndCqc.previous_non_null_value)
            * F.col(model_to_extrapolate_from)
            / F.col(IndCqc.previous_model_value),
        )

    elif extrapolation_method == "nominal":
        df = df.withColumn(
            IndCqc.extrapolation_forwards,
            F.col(IndCqc.previous_non_null_value)
            + F.col(model_to_extrapolate_from)
            - F.col(IndCqc.previous_model_value),
        )

    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

    df = df.drop(IndCqc.previous_non_null_value, IndCqc.previous_model_value)

    return df


def extrapolation_backwards(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    window_spec: Window,
    extrapolation_method: str,
) -> DataFrame:
    """
    Calculates the backward extrapolation and adds it as a new column 'extrapolation_backwards'.

    This function fills null values occurring before the first known value by extrapolating backwards.

    The ratio method is based on multiplying the rate of change from the modelled value before the
    first known non-null value and the modelled value at the first known non-null timestamp.

    The nominal method is based on adding/subtracting the difference between those two modelled values.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The name of the column representing the model to extrapolate from.
        window_spec (Window): The window specification to use for the calculation.
        extrapolation_method (str): The choice of method. Must be either 'nominal' or 'ratio'.

    Returns:
        DataFrame: The DataFrame with the added 'extrapolation_backwards' column.

    Raises:
        ValueError: If chosen extrapolation_method does not match 'nominal' or 'ratio'.
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

    if extrapolation_method == "ratio":
        df = df.withColumn(
            IndCqc.extrapolation_backwards,
            F.when(
                F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time),
                F.col(IndCqc.first_non_null_value)
                * F.col(model_to_extrapolate_from)
                / F.col(IndCqc.first_model_value),
            ),
        )

    elif extrapolation_method == "nominal":
        df = df.withColumn(
            IndCqc.extrapolation_backwards,
            F.when(
                F.col(IndCqc.unix_time) < F.col(IndCqc.first_submission_time),
                F.col(IndCqc.first_non_null_value)
                + F.col(model_to_extrapolate_from)
                - F.col(IndCqc.first_model_value),
            ),
        )

    else:
        raise ValueError("Error: method must be either 'ratio' or 'nominal'.")

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
