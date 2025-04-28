from dataclasses import dataclass

from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class TrainLinearRegressionModelSchema:
    feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )


@dataclass
class ModelMetrics:
    model_metrics_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.features, VectorUDT(), True),
        ]
    )


@dataclass
class RunLinearRegressionModelSchema:
    feature_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), False),
            StructField(IndCQC.number_of_beds, IntegerType(), True),
        ]
    )
