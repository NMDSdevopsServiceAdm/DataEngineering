from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def count_registered_manager_names(df: DataFrame) -> DataFrame:
    """
    Adds a column with a count of elements within list of registered manager names.

    This function uses the size method to count elements in list of strings. This method
    returns the count, including null elements within a list, when list partially populated.
    It returns 0 when list is empty. It returns -1 when column is null.
    Therefore, after the counting, this function recodes values of -1 to 0.

    Args:
        df (DataFrame): A dataframe containing list of registered manager names.

    Returns:
        DataFrame: A dataframe with count of elements in list of registered manager names.
    """

    df = df.withColumn(
        IndCQC.registered_manager_count, F.size(F.col(IndCQC.registered_manager_names))
    )

    df = df.withColumn(
        IndCQC.registered_manager_count,
        F.when(F.col(IndCQC.registered_manager_count) == -1, F.lit(0)).otherwise(
            F.col(IndCQC.registered_manager_count)
        ),
    )

    return df
