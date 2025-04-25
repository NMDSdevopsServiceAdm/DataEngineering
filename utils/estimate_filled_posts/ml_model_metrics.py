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
        mode="append",
        partitionKeys=[IndCqc.model_name, IndCqc.model_version, IndCqc.run_number],
    )


def generate_metric(
    evaluator: RegressionEvaluator, predictions_df: DataFrame, metric_name: str
):
    metric_value = F.round(
        evaluator.evaluate(predictions_df, {evaluator.metricName: metric_name}), 4
    )
    print(f"Calculating {metric_name} = {metric_value}")
    return metric_value


def calculate_residual_between_predicted_and_known_filled_posts(
    predictions_df: DataFrame,
    dependent_variable: str,
    is_care_home_model: bool = False,
):
    if is_care_home_model:
        prediction_col = F.col(IndCqc.prediction) * F.col(IndCqc.number_of_beds)
    else:
        prediction_col = F.col(IndCqc.prediction)

    predictions_df = predictions_df.withColumn(
        IndCqc.residual,
        (F.col(dependent_variable) - prediction_col),
    )
    return predictions_df


def generate_proportion_of_predictions_within_range(
    predictions_df: DataFrame, dependent_variable: str, range_cutoff: int
):
    within_range: str = "within_range"
    predictions_df = predictions_df.withColumn(
        within_range,
        (F.abs(F.col(IndCqc.residual)) <= range_cutoff).cast(IntegerType),
    )

    percentage_of_predictions_in_range = F.round(
        predictions_df.agg(F.sum(within_range)).collect()[0][0]
        / predictions_df.agg(F.count(dependent_variable)).collect()[0][0],
        4,
    )

    return percentage_of_predictions_in_range


def store_model_metrics(
    predictions_df: DataFrame,
    dependent_variable: str,
    model_source: str,
    model_evaluator: RegressionEvaluator,
    is_care_home_model: bool = False,
) -> Tuple[DataFrame, str]:
    spark = utils.get_spark()

    predictions_df = calculate_residual_between_predicted_and_known_filled_posts(
        predictions_df, dependent_variable, is_care_home_model
    )

    r2_value = generate_metric(
        model_evaluator,
        predictions_df,
        dependent_variable,
        IndCqc.r2,
    )
    rmse_value = generate_metric(
        model_evaluator,
        predictions_df,
        dependent_variable,
        IndCqc.rmse,
    )
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
        fields=[
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


def get_model_name_and_version_from_s3_filepath(model_source: str):
    split_filepath = model_source.split("/")

    model_name = split_filepath[-3]
    model_version = split_filepath[-2]
    run_number = split_filepath[-1]

    return model_name, model_version, run_number
