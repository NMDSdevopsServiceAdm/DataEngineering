from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc


def save_model_metrics(
    predictions_df: DataFrame,
    dependent_variable_column_name: str,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    spark = utils.get_spark()

    r2_value = generate_r2_metric(
        predictions_df,
        IndCqc.prediction,
        dependent_variable_column_name,
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
        ]
    )
    metrics_row = [(model_name, model_version, run_number, r2_value)]
    metrics_df = spark.createDataFrame(metrics_row, metrics_schema)
    metrics_df = metrics_df.withColumn(
        IndCqc.model_run_timestamp, F.current_timestamp()
    )

    print(f"Writing metrics for {model_name} as parquet to {metrics_destination}")

    utils.write_to_parquet(
        metrics_df,
        metrics_destination,
        mode="append",
        partitionKeys=[IndCqc.model_name, IndCqc.model_version, IndCqc.run_number],
    )
    # TODO adjust previous data in s3 to match new format with run number (set previous ones to 0?)


def generate_r2_metric(df: DataFrame, prediction: str, label: str):
    model_evaluator = RegressionEvaluator(
        predictionCol=prediction, labelCol=label, metricName=IndCqc.r2
    )

    r2 = model_evaluator.evaluate(df)
    print("Calculating R Squared (R2) = %g" % r2)

    return r2


def get_model_name_and_version_from_s3_filepath(model_source: str):
    split_filepath = model_source.split("/")

    model_name = split_filepath[-3]
    model_version = split_filepath[-2]
    run_number = split_filepath[-1]

    return model_name, model_version, run_number
