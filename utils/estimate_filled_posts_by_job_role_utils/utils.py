from pyspark.sql import DataFrame, functions as F

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


def merge_dataframes(posts_df: DataFrame, workers_df: DataFrame) -> DataFrame:
    """
    Joining the IndCQC Estimates dataframe (Left Table) and the ASCWDS Worker DataFrame (Right Table) together.
    All the columns from IndCQC and the Job Count columns from ASCWDS.

    Args:
        posts_df (DataFrame): A dataframe containing cleaned IndCQC workplace Data.
        workers_df (DataFrame): ASC-WDS worker dataframe grouped to include columns with job role counts per job role. .

    Returns:
        DataFrame: The IndCQC DataFrame merged to include job role count columns.
    """

    joined_df = posts_df.join(
        workers_df,
        (posts_df[IndCQC.establishment_id] == workers_df[AWKClean.establishment_id])
        & (
            posts_df[IndCQC.ascwds_workplace_import_date]
            == workers_df[AWKClean.ascwds_worker_import_date]
        ),
        "left",
    ).drop(workers_df[AWKClean.establishment_id])

    result_df = joined_df.fillna(0, subset=list_of_job_roles)

    return result_df
