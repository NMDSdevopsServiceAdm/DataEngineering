from pyspark.sql import DataFrame, functions as F

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

list_of_job_roles = list(AscwdsWorkerValueLabelsMainjrid.labels_dict.values())


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
