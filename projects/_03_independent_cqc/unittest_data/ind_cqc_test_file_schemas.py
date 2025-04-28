from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class ModelMetrics:
    generate_metric_schema = StructType(
        [
            StructField(IndCQC.location_id, StringType(), True),
            StructField(IndCQC.imputed_filled_post_model, FloatType(), True),
            StructField(IndCQC.prediction, FloatType(), True),
        ]
    )
