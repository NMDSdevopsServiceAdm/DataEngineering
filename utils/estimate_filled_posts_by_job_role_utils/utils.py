from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType
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
        list_of_job_roles (list): A list containing the ASC-WDS job role.

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


def merge_dataframes(
    estimated_filled_posts_df: DataFrame,
    aggregated_job_roles_per_establishment_df: DataFrame,
) -> DataFrame:
    """
    Join the ASC-WDS job role count column from the aggregated worker file into the estimated filled post DataFrame, matched on establishment_id and import_date.

    Args:
        estimated_filled_posts_df (DataFrame): A dataframe containing estimated filled posts at workplace level.
        aggregated_job_roles_per_establishment_df (DataFrame): ASC-WDS job role breakdown dataframe aggregated at workplace level.

    Returns:
        DataFrame: The estimated filled post DataFrame with the job role count map column joined in.
    """

    merged_df = (
        estimated_filled_posts_df.join(
            aggregated_job_roles_per_establishment_df,
            (
                estimated_filled_posts_df[IndCQC.establishment_id]
                == aggregated_job_roles_per_establishment_df[IndCQC.establishment_id]
            )
            & (
                estimated_filled_posts_df[IndCQC.ascwds_workplace_import_date]
                == aggregated_job_roles_per_establishment_df[
                    IndCQC.ascwds_worker_import_date
                ]
            ),
            "left",
        )
        .drop(aggregated_job_roles_per_establishment_df[IndCQC.establishment_id])
        .drop(
            aggregated_job_roles_per_establishment_df[IndCQC.ascwds_worker_import_date]
        )
    )

    return merged_df


def transform_job_role_count_map_to_ratios_map(
    estimated_ind_cqc_filled_posts_by_job_role_df: DataFrame,
) -> DataFrame:
    """
    Transform a job role count map column into a job role ratio map column.

    Take a map column which has keys for each job role and values are the count of each job role
    at an establishment. Make another map column with keys per job role and values as the
    percentage of each count from the total of all values in the count map.

    Args:
        estimated_ind_cqc_filled_posts_by_job_role_df (DataFrame): A dataframe containing a job role count map at workplace level.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with the job role ratio map column joined in.
    """

    temp_ascwds_total_worker_records = "temp_ascwds_total_worker_records"
    estimated_ind_cqc_filled_posts_by_job_role_df = (
        estimated_ind_cqc_filled_posts_by_job_role_df.withColumn(
            temp_ascwds_total_worker_records,
            F.aggregate(
                F.map_values(F.col(IndCQC.ascwds_job_role_counts)),
                F.lit(0).cast(LongType()),
                lambda a, b: a + b,
            ),
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        estimated_ind_cqc_filled_posts_by_job_role_df.withColumn(
            IndCQC.ascwds_job_role_ratios,
            F.map_from_arrays(
                F.map_keys(F.col(IndCQC.ascwds_job_role_counts)),
                F.transform(
                    F.map_values(F.col(IndCQC.ascwds_job_role_counts)),
                    lambda v: v / F.col(temp_ascwds_total_worker_records),
                ),
            ),
        )
    )

    return estimated_ind_cqc_filled_posts_by_job_role_df.drop(
        temp_ascwds_total_worker_records
    )


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


def sum_job_role_count_split_by_service(
    df: DataFrame, list_of_job_roles: list
) -> DataFrame:
    df_explode = df.select(
        IndCQC.primary_service_type, F.explode(IndCQC.ascwds_job_role_counts)
    )

    df_explode_grouped = (
        df_explode.groupBy(IndCQC.primary_service_type)
        .pivot("key", list_of_job_roles)
        .sum("value")
    )

    df_explode_grouped_with_map_column = df_explode_grouped.withColumn(
        IndCQC.ascwds_job_role_counts_by_primary_service,
        create_map_column(list_of_job_roles),
    ).drop(*list_of_job_roles)

    df_result = df.join(
        df_explode_grouped_with_map_column,
        df[IndCQC.primary_service_type]
        == df_explode_grouped_with_map_column[IndCQC.primary_service_type],
        "left",
    ).drop(df_explode_grouped_with_map_column[IndCQC.primary_service_type])

    return df_result
