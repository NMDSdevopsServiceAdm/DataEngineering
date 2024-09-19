from pyspark.sql import DataFrame

from utils.estimate_filled_posts.models.extrapolation import model_extrapolation
from utils.estimate_filled_posts.models.interpolation import model_interpolation


def model_extrapolation_and_interpolation(
    df: DataFrame,
    model_column_name: str,
    column_to_interpolate: str,
    new_column_name: str,
) -> DataFrame:
    """
    Adds two new columns, one with extrapolated values and one with interpolated values.

    The function prepares the dataset with features present in both models before calling each model separately.

    Args:
        df (DataFrame): A dataframe with a column to interpolate.
        model_column_name (str): The model which contains the trned for extrapolation for follow
        column_to_interpolate (str): The name of the column that needs interpolating.
        new_column_name (str): The name of the new column with interpolated values.

    Returns:
        DataFrame: A dataframe with a new column containing interpolated values.
    """
    df = model_extrapolation(df, model_column_name)

    df = model_interpolation(df, column_to_interpolate, new_column_name)

    return df
