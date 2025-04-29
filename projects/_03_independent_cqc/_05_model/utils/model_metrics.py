from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc

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

    predictions_df = trained_model.transform(test_df)

    model_evaluator = RegressionEvaluator(
        predictionCol=model_name, labelCol=dependent_variable
    )

    predictions_df = calculate_residual_between_predicted_and_known_filled_posts(
        predictions_df, model_name
    )
    r2_value = generate_metric(model_evaluator, predictions_df, IndCqc.r2)
    rmse_value = generate_metric(model_evaluator, predictions_df, IndCqc.rmse)
    prediction_within_10_posts = generate_proportion_of_predictions_within_range(
        predictions_df, range_cutoff=10
    )
    prediction_within_25_posts = generate_proportion_of_predictions_within_range(
        predictions_df, range_cutoff=25
    )

    current_metrics_row = [
        (
            model_name,
            model_version,
            model_run_number,
            r2_value,
            rmse_value,
            prediction_within_10_posts,
            prediction_within_25_posts,
        )
    ]
    current_metrics_df = spark.createDataFrame(current_metrics_row, metrics_schema)

    current_metrics_df = current_metrics_df.withColumn(
        IndCqc.model_run_timestamp, F.current_timestamp()
    )

    all_metrics_df = combine_current_and_previous_metrics(
        current_metrics_df, previous_metrics_df
    )

    print(f"Writing metrics for {model_name} as parquet to {metrics_s3_path}")

    utils.write_to_parquet(
        all_metrics_df,
        metrics_s3_path,
        mode="overwrite",
        partitionKeys=[IndCqc.model_name, IndCqc.model_version, IndCqc.run_number],
    )


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


def calculate_residual_between_predicted_and_known_filled_posts(
    predictions_df: DataFrame,
    model_name: str,
) -> DataFrame:
    """
    Adds a residual column to the predictions DataFrame.

    Args:
        predictions_df (DataFrame): DataFrame containing predictions.
        model_name (str): The name of the model to train.

    Returns:
        DataFrame: A DataFrame with residual column.
    """
    care_home_identifier: str = "care_home"

    if care_home_identifier in model_name:
        prediction_col = F.col(model_name) * F.col(IndCqc.number_of_beds)
    else:
        prediction_col = F.col(model_name)

    predictions_df = predictions_df.withColumn(
        IndCqc.residual,
        F.col(IndCqc.imputed_filled_post_model) - prediction_col,
    )
    return predictions_df


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


def generate_proportion_of_predictions_within_range(
    predictions_df: DataFrame, range_cutoff: int
) -> float:
    """
    Calculates the proportion of residuals within a given range.

    Args:
        predictions_df (DataFrame): DataFrame with residuals.
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
    total_count = predictions_df.agg(F.count(within_range)).first()[0]

    return round(in_range_count / total_count, 4)


def combine_current_and_previous_metrics(
    current_metrics_df: DataFrame, previous_metrics_df: DataFrame
) -> DataFrame:
    """
    Combines the current metrics DataFrame with the previous metrics DataFrame.

    Union by name is used in case we add new metrics in the future.
    This will ensure that the previous metrics are kept even if the schema changes.

    Args:
        current_metrics_df (DataFrame): DataFrame containing the current model metrics.
        previous_metrics_df (DataFrame): DataFrame containing the previous model metrics.

    Returns:
        DataFrame: A DataFrame containing the combined metrics.
    """
    return current_metrics_df.unionByName(previous_metrics_df, allowMissingColumns=True)
