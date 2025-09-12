from dataclasses import dataclass

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    DimensionPartitionKeys as DimensionKeys,
)


@dataclass
class JoinDimensionSchemas:
    join_dimension_with_simple_equivalence_cqc_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("sample_field", StringType(), True),
            StructField(Keys.import_date, StringType(), True),
        ]
    )

    join_dimension_with_simple_equivalence_dim_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("dimension_field", StringType(), True),
            StructField(DimensionKeys.import_date, StringType(), True),
        ]
    )

    expected_join_dimension_with_simple_equivalence_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField("sample_field", StringType(), True),
            StructField(DimensionKeys.import_date, StringType(), True),
            StructField("dimension_field", StringType(), True),
        ]
    )
