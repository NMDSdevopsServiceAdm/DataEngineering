import numpy as np
from sklearn.metrics import r2_score, root_mean_squared_error


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
