from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def count_registered_manager_names(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        IndCQC.registered_manager_count, F.size(F.col(IndCQC.registered_manager_names))
    )

    # The size method returns -1 when sizing null.
    df = df.withColumn(
        IndCQC.registered_manager_count,
        F.when(F.col(IndCQC.registered_manager_count) == -1, F.lit(0)).otherwise(
            F.col(IndCQC.registered_manager_count)
        ),
    )

    return df
