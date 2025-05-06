from dataclasses import dataclass


from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


calculate_schema = StructType(
    [
        StructField(IndCQC.location_id, StringType(), False),
        StructField("column_1", DoubleType(), True),
        StructField("column_2", DoubleType(), True),
    ]
)
expected_calculate_schema = StructType(
    [
        *calculate_schema,
        StructField("new_column", DoubleType(), True),
    ]
)
