from pyspark.sql import DataFrame
from typing import Tuple

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.estimate_filled_posts.models.extrapolation_new import model_extrapolation
from utils.estimate_filled_posts.models.interpolation_new import model_interpolation


def model_imputation_with_extrapolation_and_interpolation(
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
    ) = create_new_column_names(column_with_null_values, model_column_name)

    df = model_extrapolation(
        df,
        column_with_null_values,
        model_column_name,
        extrapolation_model_column_name,
    )
    df = model_interpolation(
        df,
        column_with_null_values,
        interpolation_model_column_name,
    )
    df = df.drop(
        IndCqc.extrapolation_backwards,
        IndCqc.extrapolation_forwards,
        IndCqc.extrapolation_residual,
        IndCqc.proportion_of_time_between_submissions,
    )

    return df


def create_new_column_names(
    column_with_null_values: str, model_column_name: str
) -> Tuple[str, str]:
    """
    Generate new column names for extrapolation and interpolation outputs.

    Generates new column names for extrapolation and interpolation outputs which includes the name of the column with
    null values and the model name which trend line the process is following. For example:
        - 'filled_posts' using the 'rolling_average_model' trend would become
          'extrapolation_filled_posts_rolling_average_model".
        - 'filled_posts_per_bed_ratio' using the 'care_home_model' trend would become
          'extrapolation_filled_posts_per_bed_ratio_care_home_model".


    Args:
        column_with_null_values (str): The name of the column containing null values to be extrapolated and interpolated.
        model_column_name (str): The name of the model column to use.

    Returns:
        Tuple[str, str]: A tuple containing the extrapolation model column name and the interpolation model column name.
    """
    extrapolation_model_column_name = (
        "extrapolation_" + column_with_null_values + "_" + model_column_name
    )
    interpolation_model_column_name = (
        "interpolation_" + column_with_null_values + "_" + model_column_name
    )
    return extrapolation_model_column_name, interpolation_model_column_name
