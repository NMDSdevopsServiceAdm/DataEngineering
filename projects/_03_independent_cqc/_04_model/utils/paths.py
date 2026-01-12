def generate_model_path(data_bucket: str, model: str, version: str) -> str:
    """
    Generate S3 path for model storage.

    For consistency with other S3 paths, this path takes the datasets bucket
    and converts it to the equivalent pipeline resource bucket.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.
        model (str): The name of the model.
        version (str): The version of the model (e.g. "1.0.0").

    Returns:
        str: The S3 path for the specified model and version.
    """
    resources_bucket = data_bucket[:-8] + "pipeline-resources"
    return f"s3://{resources_bucket}/models/{model}/{version}/"


def generate_ind_cqc_path(data_bucket: str) -> str:
    """
    Generate S3 path for features dataset for the specified model.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.

    Returns:
        str: The S3 path for the features dataset for the specified model.
    """
    return f"s3://{data_bucket}/domain=ind_cqc_filled_posts/dataset=ind_cqc_03_imputed_ascwds_and_pir/"


def generate_features_path(data_bucket: str, model: str) -> str:
    """
    Generate S3 path for features dataset for the specified model.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.
        model (str): The name of the model.

    Returns:
        str: The S3 path for the features dataset for the specified model.
    """
    return f"s3://{data_bucket}/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_features_{model}_polars/"


def generate_predictions_path(data_bucket: str, model: str) -> str:
    """
    Generate S3 path for model predictions.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.
        model (str): The name of the model.

    Returns:
        str: The S3 path for the predictions dataset for the specified model.
    """
    return f"s3://{data_bucket}/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_predictions_{model}/predictions.parquet"
