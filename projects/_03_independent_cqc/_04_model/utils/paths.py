def generate_model_path(resources_bucket: str, model: str, version: str) -> str:
    """
    Generate S3 path for model storage.

    Args:
        resources_bucket (str): The S3 bucket where pipeline resources are stored.
        model (str): The name of the model.
        version (str): The version of the model (e.g. "1.0.0").

    Returns:
        str: The S3 path for the specified model and version.
    """
    return f"s3://{resources_bucket}/models/{model}/{version}/"


def generate_features_path(data_bucket: str, model: str) -> str:
    """
    Generate S3 path for features dataset for the specified model.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.
        model (str): The name of the model.

    Returns:
        str: The S3 path for the features dataset for the specified model.
    """
    return f"s3://{data_bucket}/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_features_{model}/"


def generate_predictions_path(data_bucket: str, model: str) -> str:
    """
    Generate S3 path for model predictions.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.
        model (str): The name of the model.

    Returns:
        str: The S3 path for the predictions dataset for the specified model.
    """
    return f"s3://{data_bucket}/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_predictions_{model}/"
