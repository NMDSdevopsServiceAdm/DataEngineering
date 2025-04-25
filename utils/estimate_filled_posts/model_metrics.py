from typing import Tuple
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
    IntegerType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def save_model_metrics(
    trained_model: LinearRegressionModel,
    test_df: DataFrame,
    dependent_variable: str,
    model_source: str,
    metrics_destination: str,
    is_care_home_model: bool = False,
) -> None:
    """
    Saves model metrics by appending the current evaluation to previous metrics.

    Args:
        trained_model (LinearRegressionModel): Trained linear regression model.
        test_df (DataFrame): DataFrame to evaluate the model on.
        dependent_variable (str): The target variable.
        model_source (str): Path to the trained model.
        metrics_destination (str): Destination path for metrics parquet file.
        is_care_home_model (bool): Whether to scale predictions by number of beds.
    """
    previous_metrics_df = utils.read_from_parquet(metrics_destination)

    predictions_df = trained_model.transform(test_df)

    model_evaluator = RegressionEvaluator(
        predictionCol=IndCqc.prediction, labelCol=dependent_variable
    )

    current_metrics_df, model_name = store_model_metrics(
        predictions_df,
        dependent_variable,
        model_source,
        model_evaluator,
        is_care_home_model,
    )

    all_metrics_df = current_metrics_df.unionByName(previous_metrics_df)

    print(f"Writing metrics for {model_name} as parquet to {metrics_destination}")

    utils.write_to_parquet(
        all_metrics_df,
        metrics_destination,
        mode="overwrite",
        partitionKeys=[IndCqc.model_name, IndCqc.model_version, IndCqc.run_number],
    )


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


def calculate_residual_between_predicted_and_known_filled_posts(
    predictions_df: DataFrame,
    dependent_variable: str,
    is_care_home_model: bool = False,
) -> DataFrame:
    """
    Adds a residual column to the predictions DataFrame.

    Args:
        predictions_df (DataFrame): DataFrame containing predictions.
        dependent_variable (str): Actual values column.
        is_care_home_model (bool): Whether predictions should be scaled by number of beds.

    Returns:
        DataFrame: A DataFrame with residual column.
    """
    if is_care_home_model:
        prediction_col = F.col(IndCqc.prediction) * F.col(IndCqc.number_of_beds)
    else:
        prediction_col = F.col(IndCqc.prediction)

    predictions_df = predictions_df.withColumn(
        IndCqc.residual,
        F.col(dependent_variable) - prediction_col,
    )
    return predictions_df


def generate_proportion_of_predictions_within_range(
    predictions_df: DataFrame, dependent_variable: str, range_cutoff: int
) -> float:
    """
    Calculates the proportion of residuals within a given range.

    Args:
        predictions_df (DataFrame): DataFrame with residuals.
        dependent_variable (str): The target variable column name.
        range_cutoff (int): The threshold within which residuals are considered accurate.

    Returns:
        float: The rounded proportion of predictions within the given range.
    """
    within_range: str = "within_range"
    predictions_df = predictions_df.withColumn(
        within_range,
        (F.abs(F.col(IndCqc.residual)) <= range_cutoff).cast(IntegerType()),
    )

    in_range_count = predictions_df.agg(F.sum(within_range)).first()[0]
    total_count = predictions_df.agg(F.count(dependent_variable)).first()[0]

    return round(in_range_count / total_count, 4)


def store_model_metrics(
    predictions_df: DataFrame,
    dependent_variable: str,
    model_source: str,
    model_evaluator: RegressionEvaluator,
    is_care_home_model: bool = False,
) -> Tuple[DataFrame, str]:
    """
    Generates and stores model evaluation metrics.

    Args:
        predictions_df (DataFrame): DataFrame with model predictions.
        dependent_variable (str): Name of the target column.
        model_source (str): Path to the model.
        model_evaluator (RegressionEvaluator): Evaluator used for calculating metrics.
        is_care_home_model (bool): Whether predictions should be scaled by number of beds.

    Returns:
        Tuple[DataFrame, str]: The metrics DataFrame and name of the model.
    """
    spark = utils.get_spark()

    predictions_df = calculate_residual_between_predicted_and_known_filled_posts(
        predictions_df, dependent_variable, is_care_home_model
    )

    r2_value = generate_metric(model_evaluator, predictions_df, IndCqc.r2)
    rmse_value = generate_metric(model_evaluator, predictions_df, IndCqc.rmse)
    prediction_within_10_posts = generate_proportion_of_predictions_within_range(
        predictions_df, dependent_variable, range_cutoff=10
    )
    prediction_within_25_posts = generate_proportion_of_predictions_within_range(
        predictions_df, dependent_variable, range_cutoff=25
    )

    model_name, model_version, run_number = get_model_name_and_version_from_s3_filepath(
        model_source
    )

    metrics_schema = StructType(
        [
            StructField(IndCqc.model_name, StringType(), True),
            StructField(IndCqc.model_version, StringType(), True),
            StructField(IndCqc.run_number, StringType(), True),
            StructField(IndCqc.r2, FloatType(), True),
            StructField(IndCqc.rmse, FloatType(), True),
            StructField(IndCqc.prediction_within_10_posts, FloatType(), True),
            StructField(IndCqc.prediction_within_25_posts, FloatType(), True),
        ]
    )
    metrics_row = [
        (
            model_name,
            model_version,
            run_number,
            r2_value,
            rmse_value,
            prediction_within_10_posts,
            prediction_within_25_posts,
        )
    ]
    metrics_df = spark.createDataFrame(metrics_row, metrics_schema)

    metrics_df = metrics_df.withColumn(
        IndCqc.model_run_timestamp, F.current_timestamp()
    )
    return metrics_df, model_name


def get_model_name_and_version_from_s3_filepath(
    model_source: str,
) -> Tuple[str, str, str]:
    """
    Parses S3 file path to extract model name, version and run number.

    Args:
        model_source (str): S3 path to the model.

    Returns:
        Tuple[str, str, str]: The model_name, model_version and run_number.
    """
    split_filepath = model_source.strip("/").split("/")

    return split_filepath[-3], split_filepath[-2], split_filepath[-1]
