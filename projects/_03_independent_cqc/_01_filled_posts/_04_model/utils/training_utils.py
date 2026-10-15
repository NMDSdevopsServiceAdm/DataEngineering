import numpy as np
import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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


def is_training_row(dependent_column: str) -> pl.Expr:
    """
    Expression for the rows a model is trained on.

    A row needs a known dependent value, and its location must only ever have had one care
    home status.

    Args:
        dependent_column (str): the dependent (target) column of the model

    Returns:
        pl.Expr: true for rows to train on
    """
    return pl.col(dependent_column).is_not_null() & (
        pl.col(IndCQC.care_home_status_count) == 1
    )
