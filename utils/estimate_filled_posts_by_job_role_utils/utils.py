from pyspark.sql import DataFrame, functions as F
from typing import List

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

list_of_job_roles = list(AscwdsWorkerValueLabelsMainjrid.labels_dict.values())


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


def count_job_role_per_establishment_as_columns(
    df: DataFrame, list_of_columns_for_job_role: list
) -> DataFrame:
    """
    Group the worker dataset by establishment id and import date.
    Subsequently performs a pivot on the clean job role labels which will be the additional columns in the grouped data.
    Any labels in the list of job roles not present in the establishment will have their counts aggregated to zero.

    Args:
        df (DataFrame): A dataframe containing cleaned ASC-WDS worker data.
        list_of_columns_for_job_role (list): A list containing the ASC-WDS job role.

    Returns:
        DataFrame: A dataframe with unique establishmentid and import date.
    """
    df = (
        df.groupBy(
            F.col(AWKClean.establishment_id),
            F.col(AWKClean.ascwds_worker_import_date),
        )
        .pivot(AWKClean.main_job_role_clean_labelled, list_of_columns_for_job_role)
        .count()
    )

    df = df.na.fill(0, subset=list_of_columns_for_job_role)

    for column in list_of_columns_for_job_role:
        df = df.withColumnRenamed(column, f"job_role_count_{column}")

    return df


def transform_job_role_counts_to_ratios(
    df: DataFrame, list_of_job_role_columns: List[str]
) -> DataFrame:
    """
    Adds columns with the ratio of each job role from given list of job role count columns.

    Args:
        df (DataFrame): A dataframe containing job role count columns.
        list_of_job_role_columns (List[str]): List of job role count columns.

    Returns:
        DataFrame: A dataframe with additional job role ratio columns.
    """

    temp_job_role_count_total_column = "temp_job_role_count_total_column"
    df = df.withColumn(
        temp_job_role_count_total_column,
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
            F.when(
                F.col(column) > 0,
                F.col(column) / F.col(temp_job_role_count_total_column),
            )
            .when(F.col(temp_job_role_count_total_column).isNull(), None)
            .otherwise(F.lit(0.0)),
        )

    return df.drop(temp_job_role_count_total_column)
