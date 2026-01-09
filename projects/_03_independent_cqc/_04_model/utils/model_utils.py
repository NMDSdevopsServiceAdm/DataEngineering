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
    predictions: np.ndarray,
    index_col: str,
    model_name: str,
    model_version: str,
    run_number: int,
) -> pl.DataFrame:
    """
    Add model predictions and metadata to a Polars DataFrame.

    This function links predictions to rows by a unique index column to avoid
    misalignment. It also tags each prediction with a model version and run ID.

    Args:
        df (pl.DataFrame): Input Polars DataFrame.
        predictions (np.ndarray): Array of prediction values (must match df row count).
        index_col (str): Name of the unique index column for joining predictions.
        model_name (str): The name of the model (used as the new column name).
        model_version (str): Model version string.
        run_number (int): Model run number.

    Returns:
        pl.DataFrame: DataFrame containing `location_id`, `cqc_location_import_date`,
                      predictions column, and predictions run ID column.

    Raises:
        ValueError: If the length of predictions does not match the number of rows in df.
    """
    if len(predictions) != df.height:
        raise ValueError(
            f"Predictions length ({len(predictions)}) does not match DataFrame row count ({df.height})"
        )

    predictions_col = f"{model_name}_predictions"
    run_id_col = f"{predictions_col}_run_id"
    run_id_value = f"{model_name}_v{model_version}_r{run_number}"

    predictions_df = df.select(index_col).with_columns(
        pl.Series(predictions_col, predictions),
        pl.lit(run_id_value).alias(run_id_col),
    )

    df_with_predictions = df.join(predictions_df, on=index_col, how="left")

    return df_with_predictions.select(
        IndCQC.location_id,
        IndCQC.cqc_location_import_date,
        predictions_col,
        run_id_col,
    )
