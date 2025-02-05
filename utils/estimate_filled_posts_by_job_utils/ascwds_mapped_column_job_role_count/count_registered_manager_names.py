from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def count_registered_manager_names(df: DataFrame) -> DataFrame:
    return df
