def generate_model_metrics_s3_path(
    branch_name: str, model_name: str, model_version: str
) -> str:
    """
    Generate the S3 path for the features dataset.

    Args:
        branch_name (str): The name of the branch currently being used.
        model_name (str): The name of the model.
        model_version (str): The version of the model to use (e.g. '1.0.0').

    Returns:
        str: The S3 path for the features dataset.
    """
    return f"s3://sfc-{branch_name}-datasets/domain=ind_cqc_filled_posts/dataset=ind_cqc_model_metrics/model_name={model_name}/model_version={model_version}/"
