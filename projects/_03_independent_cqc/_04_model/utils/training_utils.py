import numpy as np
import polars as pl


def convert_dataframe_to_numpy(
    df: pl.DataFrame, feature_columns: list[str], dependent_column: str
) -> tuple[np.ndarray, np.ndarray]:
    """
    Converts Polars DataFrame to NumPy arrays for features and target.

    `ravel()` is required when converting the dependent column `y` into a 1D array
    (required for ML libraries).

    An error will be raised if any of the specified columns do not exist in the DataFrame.

    Args:
        df (pl.DataFrame): Input DataFrame.
        feature_columns (list[str]): List of feature column names.
        dependent_column (str): Name of dependent column name.

    Returns:
        tuple[np.ndarray, np.ndarray]: A tuple containing the features and target array.
    """
    X = df.select(feature_columns).to_numpy()
    y = df.select(dependent_column).to_numpy().ravel()

    return X, y
