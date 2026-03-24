def generate_predictions_path(data_bucket: str, model: str) -> str:
    """
    Generate S3 path for model predictions.

    Args:
        data_bucket (str): The S3 bucket where datasets are stored.
        model (str): The name of the model.

    Returns:
        str: The S3 path for the predictions dataset for the specified model.
    """
    return f"s3://{data_bucket}/domain=ind_cqc_filled_posts/dataset=ind_cqc_04_predictions_{model}"
