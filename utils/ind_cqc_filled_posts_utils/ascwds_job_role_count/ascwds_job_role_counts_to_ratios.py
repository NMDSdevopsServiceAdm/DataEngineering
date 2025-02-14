from pyspark.sql import DataFrame, functions as F
from typing import List


def transform_job_role_counts_to_ratios(
    df: DataFrame, list_of_job_role_columns: List[str]
) -> DataFrame:
    """
    Adds columns with the ratio of each job role from given list of job role count columns.

    Args:
        df (DataFrame): A dataframe containing job role count columns.
        list_of_job_role_columns (list): List of job role count columns.

    Returns:
        DataFrame: A dataframe with additional job role ratio columns.
    """

    temp_total_column = "temp_total_column"
    df = df.withColumn(
        temp_total_column,
        F.when(
            F.greatest(
                *[F.col(column) for column in list_of_job_role_columns]
            ).isNotNull(),
            sum(
                F.coalesce(F.col(column), F.lit(0))
                for column in list_of_job_role_columns
            ),
        ).otherwise(F.lit(None)),
    )

    for column in list_of_job_role_columns:
        df = df.withColumn(
            column.replace("count", "ratio"),
            F.when(F.col(column) > 0, F.col(column) / F.col(temp_total_column))
            .when(F.col(temp_total_column).isNull(), None)
            .otherwise(F.lit(0.0)),
        )

    return df.drop(temp_total_column)
