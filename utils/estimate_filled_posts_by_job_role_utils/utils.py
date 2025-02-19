from pyspark.sql import DataFrame, functions as F
from typing import List

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

list_of_job_roles = list(AscwdsWorkerValueLabelsMainjrid.labels_dict.values())


def aggregate_ascwds_worker_job_roles_per_establishment(
    df: DataFrame, list_of_job_roles: list
) -> DataFrame:
    """
    Aggregates the worker dataset by establishment_id and import date and transforms them into a mapped structure.

    This function aggregates the worker dataset by establishment_id and import date and creates a pivot table that has one column per job role counts.
    Null values are populated with zero before creating a map column that contains job role names and counts.
    The individual job role count columns are then dropped.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data.
        list_of_columns_for_job_role (list): A list containing the ASC-WDS job role.

    Returns:
        DataFrame: A dataframe with unique establishmentid and import date.
    """
    df = (
        df.groupBy(
            F.col(IndCQC.establishment_id), F.col(IndCQC.ascwds_worker_import_date)
        )
        .pivot(IndCQC.main_job_role_clean_labelled, list_of_job_roles)
        .count()
    )
    df = df.na.fill(0, subset=list_of_job_roles)

    df = df.withColumn(
        IndCQC.ascwds_job_role_counts, create_map_column(list_of_job_roles)
    )

    df = df.drop(*list_of_job_roles)

    return df


def create_map_column(columns: List[str]) -> F.Column:
    """
    Creates a Spark map column from a list of columns where keys are column names and values are the respective column values.

    Args:
        columns (List[str]): List of column names to be mapped.

    Returns:
        F.Column: A Spark column containing a map of job role names to counts.
    """
    return F.create_map(*[x for col in columns for x in (F.lit(col), F.col(col))])


def count_registered_manager_names(df: DataFrame) -> DataFrame:
    """
    Adds a column with a count of elements within list of registered manager names.

    This function uses the size method to count elements in list of strings. This method
    returns the count, including null elements within a list, when list partially populated.
    It returns 0 when list is empty. It returns -1 when row is null.
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
