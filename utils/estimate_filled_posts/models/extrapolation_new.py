from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def model_extrapolation(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    extrapolated_column_name: str,
    window_spec: Window,
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
        window_spec (Window): The window specification for partitioning the data.

    Returns:
        DataFrame: The DataFrame with the extrapolated values in the specified column.
    """
    df = calculate_first_and_last_submission_dates(
        df, column_with_null_values, window_spec
    )

    df = extrapolation_forwards(
        df, column_with_null_values, model_to_extrapolate_from, window_spec
    )

    df = extrapolation_backwards(
        df, column_with_null_values, model_to_extrapolate_from, window_spec
    )

    df = combine_extrapolation(df, extrapolated_column_name)

    return df


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
    window_spec_all_data = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    df = df.withColumn(
        IndCqc.first_submission_time,
        F.min(
            F.when(
                F.col(column_with_null_values).isNotNull(),
                F.col(IndCqc.unix_time),
            )
        ).over(window_spec_all_data),
    )
    df = df.withColumn(
        IndCqc.last_submission_time,
        F.max(
            F.when(
                F.col(column_with_null_values).isNotNull(),
                F.col(IndCqc.unix_time),
            )
        ).over(window_spec_all_data),
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
    df = calculate_lagged_column_value_when_data_last_submitted(
        df,
        column_with_null_values,
        column_with_null_values,
        IndCqc.previous_non_null_value,
        window_spec,
    )
    df = calculate_lagged_column_value_when_data_last_submitted(
        df,
        column_with_null_values,
        model_to_extrapolate_from,
        IndCqc.previous_model_value,
        window_spec,
    )

    df = calculate_extrapolation_forwards(df, model_to_extrapolate_from)

    df = df.drop(IndCqc.previous_non_null_value, IndCqc.previous_model_value)

    return df


def calculate_lagged_column_value_when_data_last_submitted(
    df: DataFrame,
    column_with_null_values: str,
    column_name: str,
    new_column_name: str,
    window_spec: Window,
) -> DataFrame:
    """
    Calculates a lagged column value based on the last non-null value of a specified column

    Calculates a lagged column value based on the last non-null value of a specified column
    when '<column_with_null_values>' is not null, and assigns it to a new column.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        column_name (str): The name of the column to use for calculating the lagged value.
        new_column_name (str): The name of the new column to store the lagged value.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: The DataFrame with the new lagged column.
    """
    df = df.withColumn(
        new_column_name,
        F.last(
            F.when(
                F.col(column_with_null_values).isNotNull(),
                F.col(column_name),
            ),
            ignorenulls=True,
        ).over(window_spec.rowsBetween(Window.unboundedPreceding, -1)),
    )

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
    df = get_first_non_null_value(df, column_with_null_values, window_spec)
    df = get_first_model_value_when_column_with_null_values_is_not_null(
        df, column_with_null_values, model_to_extrapolate_from, window_spec
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


def get_first_non_null_value(
    df: DataFrame, column_with_null_values: str, window_spec: Window
) -> DataFrame:
    """
    Adds a column 'first_non_null_value' with the first non-null value of '<column_with_null_values>' over the entire window.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: The DataFrame with the added 'first_non_null_value' column.
    """
    window_spec_all_data = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    df = df.withColumn(
        IndCqc.first_non_null_value,
        F.first(F.col(column_with_null_values), ignorenulls=True).over(
            window_spec_all_data
        ),
    )
    return df


def get_first_model_value_when_column_with_null_values_is_not_null(
    df: DataFrame,
    column_with_null_values: str,
    model_to_extrapolate_from: str,
    window_spec: Window,
) -> DataFrame:
    """
    Adds a column 'first_model_value' with the minimum non-null value of the specified model column over the entire window.

    Args:
        df (DataFrame): The input DataFrame.
        column_with_null_values (str): The name of the column with null values in.
        model_to_extrapolate_from (str): The name of the column representing the model to extrapolate from.
        window_spec (Window): The window specification to use for the calculation.

    Returns:
        DataFrame: The DataFrame with the added 'first_model_value' column.
    """
    window_spec_all_data = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    df = df.withColumn(
        IndCqc.first_model_value,
        F.first(
            F.when(
                F.col(column_with_null_values).isNotNull(),
                F.col(model_to_extrapolate_from),
            ),
            ignorenulls=True,
        ).over(window_spec_all_data),
    )
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
