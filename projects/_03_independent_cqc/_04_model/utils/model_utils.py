import numpy as np
import polars as pl
from sklearn.linear_model import Lasso, LinearRegression
from sklearn.metrics import r2_score, root_mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from projects._03_independent_cqc._04_model.utils.value_labels import ModelTypes
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def build_model(
    model_type: str, model_params: dict | None = None
) -> LinearRegression | Pipeline:
    """
    Returns a scikit-learn model or pipeline based on model_type.

    Feature scaling is applied to lasso regression models using a scikit-learn Pipeline to
    ensure that all features contribute equally to the regularisation process.

    Args:
        model_type (str): The type of model to build. Supported types are "linear_regression" and "lasso".
        model_params (dict | None): Optional. A dictionary of parameters to pass to the model constructor.

    Returns:
        LinearRegression | Pipeline: The constructed scikit-learn model or pipeline.

    Raises:
        ValueError: If an unsupported model_type is provided.
    """
    if model_params is None:
        model_params = {}

    if model_type == ModelTypes.linear_regression:
        return LinearRegression(**model_params)

    elif model_type == ModelTypes.lasso:
        return Pipeline(
            [("scaler", StandardScaler()), (ModelTypes.lasso, Lasso(**model_params))]
        )

    else:
        raise ValueError(f"Unknown model type: {model_type}")


def calculate_metrics(y_known: np.ndarray, y_predicted: np.ndarray) -> dict:
    """
    Calculate R2 and RMSE metrics for model evaluation.

    Args:
        y_known (np.ndarray): Known target values.
        y_predicted (np.ndarray): Predicted target values from the model.

    Returns:
        dict: A dictionary containing R2 and RMSE metrics.
    """
    r2_metric = float(r2_score(y_known, y_predicted))
    rmse_metric = float(root_mean_squared_error(y_known, y_predicted))

    return {"r2": r2_metric, "rmse": rmse_metric}


def create_predictions_dataframe(
    df: pl.DataFrame,
    predictions: pl.Series,
    model_name: str,
    model_version: str,
    run_number: int,
) -> pl.DataFrame:
    """
    Creates a predictions DataFrame by adding prediction values to the input DataFrame including metadata.

    Args:
        df (pl.DataFrame): Input Polars DataFrame.
        predictions (pl.Series): List of prediction values (must match df row count).
        model_name (str): The name of the model (used as the new column name).
        model_version (str): The version of the model.
        run_number (int): The run number of the model.

    Returns:
        pl.DataFrame: DataFrame with predictions column added and relevant columns selected.

    Raises:
        ValueError: If the length of predictions does not match the number of rows in df.
    """
    if len(predictions) != df.height:
        raise ValueError(
            f"Predictions length ({len(predictions)}) does not match DataFrame row count ({df.height})"
        )

    model_col_name = f"{model_name}_predictions"
    model_run_id_col_name = f"{model_col_name}_run_id"
    run_id = f"{model_name}_v{model_version}_r{run_number}"

    return df.select(IndCQC.location_id, IndCQC.cqc_location_import_date).with_columns(
        pl.Series(name=model_col_name, values=predictions),
        pl.lit(run_id).alias(model_run_id_col_name),
    )
