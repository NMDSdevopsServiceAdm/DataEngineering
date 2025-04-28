from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame


def generate_metric(
    evaluator: RegressionEvaluator, predictions_df: DataFrame, metric_name: str
) -> float:
    """
    Evaluates a single metric from the model predictions.

    Args:
        evaluator (RegressionEvaluator): RegressionEvaluator object.
        predictions_df (DataFrame): DataFrame containing predictions.
        metric_name (str): Metric to evaluate ('r2', 'rmse', etc.).

    Returns:
        float: The rounded metric value.
    """
    metric_value = round(
        evaluator.evaluate(predictions_df, {evaluator.metricName: metric_name}), 4
    )
    print(f"Calculating {metric_name} = {metric_value}")
    return metric_value
