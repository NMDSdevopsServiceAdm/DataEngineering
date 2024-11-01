from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
)

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)


def save_model_metrics(
    predictions_df: DataFrame,
    dependent_variable_column_name: str,
    model_source: str,
    metrics_destination: str,
) -> DataFrame:
    spark = utils.get_spark()

    data_for_metrics = get_predictions_with_known_dependent_variable_df(
        predictions_df, dependent_variable_column_name
    )
    r2_value = generate_r2_metric(
        data_for_metrics,
        IndCqc.prediction,
        dependent_variable_column_name,
    )

    model_name, model_version = get_model_name_and_version_from_s3_filepath(
        model_source
    )

    metrics_schema = StructType(
        fields=[
            StructField(IndCqc.model_name, StringType(), True),
            StructField(IndCqc.model_version, StringType(), True),
            StructField(IndCqc.r2, FloatType(), True),
        ]
    )
    metrics_row = [(model_name, model_version, r2_value)]
    metrics_df = spark.createDataFrame(metrics_row, metrics_schema)
    metrics_df = metrics_df.withColumn(
        IndCqc.model_run_timestamp, F.current_timestamp()
    )

    print(f"Writing metrics for {model_name} as parquet to {metrics_destination}")

    utils.write_to_parquet(
        metrics_df,
        metrics_destination,
        mode="append",
        partitionKeys=[IndCqc.model_name, IndCqc.model_version],
    )


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

    return model_name, model_version


def get_predictions_with_known_dependent_variable_df(
    predictions_df: DataFrame,
    dependent_variable_column_name: str,
) -> DataFrame:
    return predictions_df.where(F.col(dependent_variable_column_name).isNotNull())
