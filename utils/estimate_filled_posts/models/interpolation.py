from pyspark.sql import DataFrame, functions as F, Window
from typing import Optional, Tuple
import typing

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


def model_interpolation(
    df: DataFrame,
    column_with_null_values: str,
    method: str,
    new_column_name: Optional[str] = IndCqc.interpolation_model,
    partition_columns: typing.Optional[typing.List[str]] = IndCqc.location_id,
) -> DataFrame:
    """
    Perform interpolation on a column with null values and adds as a new column called 'interpolation_model'.

    This function can produce two styles of interpolation:
        - straight line interpolation
        - trend line interpolation (as part of imputation model), where it uses the extrapolation_forwards values as a trend line to guide interpolated predictions.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_with_null_values (str): The name of the column that contains null values to be interpolated.
        method (str): The choice of method. Must be either 'straight' or 'trend'
        new_column_name (Optional[str]): The name of the new column. Default is 'interpolation_model'
        partition_columns (typing.Optional[typing.List[str]]): A list of partition column. With the default being 'location_id'

    Returns:
        DataFrame: The DataFrame with the interpolated values in the 'interpolation_model' column.

    Raises:
        ValueError: If chosen method does not match 'straight' or 'trend'.
    """

    (
        window_spec_backwards,
        window_spec_forwards,
        window_spec_lagged,
    ) = define_window_specs(partition_columns)

    df = calculate_proportion_of_time_between_submissions(
        df, column_with_null_values, window_spec_backwards, window_spec_forwards
    )

    if method == "trend":
        df = calculate_residuals(
            df,
            column_with_null_values,
            IndCqc.extrapolation_forwards,
            window_spec_forwards,
        )
        df = calculate_interpolated_values(
            df, IndCqc.extrapolation_forwards, new_column_name
        )

    elif method == "straight":
        df = get_selected_value(
            df,
            window_spec_lagged,
            column_with_null_values,
            column_with_null_values,
            IndCqc.previous_non_null_value,
            "last",
        )
        df.show()
        print("WATCH OUT FOR THIS!")
        df = calculate_residuals(
            df,
            column_with_null_values,
            IndCqc.previous_non_null_value,
            window_spec_forwards,
        )
        df.show()
        print("WATCH OUT FOR THIS!")
        df = calculate_interpolated_values(
            df, IndCqc.previous_non_null_value, new_column_name
        )
        df.show()
        print("WATCH OUT FOR THIS!")
        df = df.drop(IndCqc.previous_non_null_value)

    else:
        raise ValueError("Error: method must be either 'straight' or 'trend'")

    df = df.drop(IndCqc.proportion_of_time_between_submissions, IndCqc.residual)

    return df


def define_window_specs(
    partition_columns: typing.Optional[typing.List[str]] = IndCqc.location_id,
) -> Tuple[Window, Window, Window]:
    """
    Defines three window specifications, partitioned by 'location_id' and ordered by 'unix_time'.

    The first window specification ('window_spec_backwards') includes all rows up to the current row.
    The second window specification ('window_spec_forward') includes all rows from the current row onwards.
    The third window specification ('window_spec_lagged') includes all rows from the start of the partition up to the current row, excluding the current row.

    Args:
        partition_columns (typing.Optional[typing.List[str]]): A list of partition column. With the default being 'location_id'

    Returns:
        Tuple[Window, Window, Window]: A tuple containing the three window specifications.
    """
    window_spec = Window.partitionBy(partition_columns).orderBy(IndCqc.unix_time)

    window_spec_backwards = window_spec.rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    window_spec_forward = window_spec.rowsBetween(
        Window.currentRow, Window.unboundedFollowing
    )
    window_spec_lagged = window_spec.rowsBetween(Window.unboundedPreceding, -1)

    return window_spec_backwards, window_spec_forward, window_spec_lagged


def calculate_residuals(
    df: DataFrame, first_column: str, second_column: str, window_spec_forward: Window
) -> DataFrame:
    """
    Calculate the residual between two non-null values (first_column minus second_column).

    This function computes the residuals between two non-null values in the specified columns.
    It creates a temporary column to store the difference between the non-null values, then duplicates
    the first non-null residual over a specified window and assigns it to a new column called 'residual'.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        first_column (str): The name of the first column that contains values.
        second_column (str): The name of the second column that contains values.
        window_spec_forward (Window): The window specification for getting the next residual value.

    Returns:
        DataFrame: The DataFrame with the calculated residuals in a new column.
    """
    temp_col: str = "temp_col"
    df = df.withColumn(
        temp_col,
        F.when(
            F.col(first_column).isNotNull() & F.col(second_column).isNotNull(),
            F.col(first_column) - F.col(second_column),
        ),
    )

    df = df.withColumn(
        IndCqc.residual,
        F.when(
            F.col(second_column).isNotNull(),
            F.first(F.col(temp_col), ignorenulls=True).over(window_spec_forward),
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


def calculate_interpolated_values(
    df: DataFrame, column_to_interpolate_from: str, new_column_name: str
) -> DataFrame:
    """
    Calculate interpolated values for a new column in a DataFrame.

    This function takes a DataFrame and interpolates values from an existing column to create a new column.
    The interpolation is based on the residual and the proportion of time between submissions.

    Args:
        df (DataFrame): The input DataFrame containing the data.
        column_to_interpolate_from (str): The name of the column from which to interpolate values.
        new_column_name (str): The name of the new column to be created with interpolated values.

    Returns:
        DataFrame: A new DataFrame with the interpolated values added as a new column.
    """
    df = df.withColumn(
        new_column_name,
        F.col(column_to_interpolate_from)
        + F.col(IndCqc.residual) * F.col(IndCqc.proportion_of_time_between_submissions),
    )
    return df
