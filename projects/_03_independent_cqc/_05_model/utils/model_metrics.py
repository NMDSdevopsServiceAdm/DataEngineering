from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def save_model_metrics(
    trained_model: LinearRegressionModel,
    test_df: DataFrame,
    dependent_variable: str,
    branch_name: str,
    model_name: str,
    model_version: str,
    model_run_number: int,
) -> None:
    """
    Saves model metrics by appending the current evaluation to previous metrics.

    Args:
        trained_model (LinearRegressionModel): Trained linear regression model.
        test_df (DataFrame): DataFrame to evaluate the model on.
        dependent_variable (str): The target variable.
        branch_name (str): The name of the branch currently being used.
        model_name (str): The name of the model to train.
        model_version (str): The version of the model to use (e.g. '1.0.0').
        model_run_number (int): The run number of the model.
    """
    spark = utils.get_spark()

    metrics_s3_path = generate_model_metrics_s3_path(
        branch_name, model_name, model_version
    )

    previous_metrics_df = utils.read_from_parquet(metrics_s3_path)


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
