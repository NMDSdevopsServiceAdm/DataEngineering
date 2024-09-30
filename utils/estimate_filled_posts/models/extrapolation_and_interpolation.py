from pyspark.sql import DataFrame
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.estimate_filled_posts.models.extrapolation_new import model_extrapolation
from utils.estimate_filled_posts.models.interpolation_new import model_interpolation


def model_extrapolation_and_interpolation(
    df: DataFrame,
    column_with_null_values: str,
    model_column_name: str,
) -> DataFrame:
    """
    Extrapolates and interpolates non-null values in 'column_with_null_values' based on the rate of change of values in 'model_column_name' and adds them as new columns.

    Calculates currently null values in a specified column by using the rate of change in a specified model column to
    extrapolate and interpolate values. This process creates two additional columns: one with extrapolated
    values and one with interpolated values.

    Args:
        df (DataFrame): The input DataFrame containing the columns to be extrapolated and interpolated.
        column_with_null_values (str): The name of the column containing null values to be extrapolated and interpolated.
        model_column_name (str): The name of the column containing the model values used for extrapolation and interpolation.

    Returns:
        DataFrame: The DataFrame with the added columns for extrapolated and interpolated values.
    """
    (
        extrapolation_model_column_name,
        interpolation_model_column_name,
    ) = create_new_column_names(model_column_name)

    df = model_extrapolation(
        df,
        column_with_null_values,
        model_column_name,
        extrapolation_model_column_name,
    )
    df = model_interpolation(
        df,
        column_with_null_values,
        model_column_name,
        interpolation_model_column_name,
    )

    return df


def create_new_column_names(model_column_name: str) -> Tuple[str, str]:
    """
    Generate new column names for extrapolation and interpolation outputs.

    Args:
        model_column_name (str): The name of the model column to use.

    Returns:
        Tuple[str, str]: A tuple containing the extrapolation model column name and the interpolation model column name.
    """
    extrapolation_model_column_name = "extrapolation_" + model_column_name
    interpolation_model_column_name = "interpolation_" + model_column_name
    return extrapolation_model_column_name, interpolation_model_column_name
