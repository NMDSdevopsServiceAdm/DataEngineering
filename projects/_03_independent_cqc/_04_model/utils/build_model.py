from sklearn.linear_model import Lasso, LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def build_model(
    model_type: str, model_params: dict = {}
) -> LinearRegression | Pipeline:
    """
    Returns a scikit-learn model or pipeline based on model_type.

    Feature scaling is applied to lasso regression models using a scikit-learn Pipeline to
    ensure that all features contribute equally to the regularisation process.

    Args:
        model_type (str): The type of model to build. Supported types are "linear" and "lasso".
        model_params (dict): A dictionary of parameters to pass to the model constructor.

    Returns:
        LinearRegression | Pipeline: The constructed scikit-learn model or pipeline.

    Raises:
        ValueError: If an unsupported model_type is provided.
    """
    if model_type == "linear":
        return LinearRegression(**model_params)

    elif model_type == "lasso":
        return Pipeline(
            [("scaler", StandardScaler()), ("lasso", Lasso(**model_params))]
        )

    else:
        raise ValueError(f"Unknown model type: {model_type}")
