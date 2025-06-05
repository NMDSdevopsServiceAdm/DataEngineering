from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    IntegerType,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

new_column: str = "new_column"
column_1: str = "column_1"
column_2: str = "column_2"

calculate_new_column_schema = StructType(
    [
        StructField(IndCQC.location_id, StringType(), False),
        StructField("column_1", DoubleType(), True),
        StructField("column_2", DoubleType(), True),
    ]
)
expected_calculate_new_column_schema = StructType(
    [
        *calculate_new_column_schema,
        StructField(new_column, DoubleType(), True),
    ]
)

calculate_windowed_column_schema = StructType(
    [
        StructField(IndCQC.location_id, StringType(), False),
        StructField(IndCQC.cqc_location_import_date, DateType(), False),
        StructField(IndCQC.care_home, StringType(), False),
        StructField(IndCQC.ascwds_filled_posts, DoubleType(), True),
    ]
)
expected_calculate_windowed_column_schema = StructType(
    [
        *calculate_windowed_column_schema,
        StructField(new_column, DoubleType(), True),
    ]
)
expected_calculate_windowed_column_count_schema = StructType(
    [
        *calculate_windowed_column_schema,
        StructField(new_column, IntegerType(), True),
    ]
)
