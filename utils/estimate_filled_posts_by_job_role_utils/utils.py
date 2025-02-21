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
    count_map_column: str,
    count_map_values_total_column_name: str,
    ratio_map_column_name: str,
) -> DataFrame:
    """
    Transform a count map column into a ratio map column.

    Adds a column to hold the total of values from the given count map.
    Adds a column with the ratio map.
    Drops the column with the total of values from given count map.

    Args:
        estimated_ind_cqc_filled_posts_by_job_role_df (DataFrame): A dataframe containing a job role count map at workplace level.
        count_map_column (str): A map column of type any:long.
        count_map_values_total_column_name (str): The name to give to the total values from count map column.
        ratio_map_column_name (str): The name to give to the ratio map column.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with the job role ratio map column joined in.
    """

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        create_total_from_values_in_map_column(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            count_map_column,
            count_map_values_total_column_name,
        )
    )

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        create_ratios_map_from_count_map_and_total(
            estimated_ind_cqc_filled_posts_by_job_role_df,
            count_map_column,
            count_map_values_total_column_name,
            ratio_map_column_name,
        )
    )

    return estimated_ind_cqc_filled_posts_by_job_role_df.drop(
        IndCQC.ascwds_job_role_counts_total
    )


def create_total_from_values_in_map_column(
    estimated_ind_cqc_filled_posts_by_job_role_df: DataFrame,
    count_map_column: str,
    count_map_values_total_column_name: str,
) -> DataFrame:
    """
    Adds a column which contains the total of values from a given map column of type any:long.

    Args:
        estimated_ind_cqc_filled_posts_by_job_role_df (DataFrame): A dataframe containing a count map.
        count_map_column (str): A map column of type any:long.
        count_map_values_total_column_name (str): The name to give to the total column being added.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with a column for total of map values added.
    """

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        estimated_ind_cqc_filled_posts_by_job_role_df.withColumn(
            count_map_values_total_column_name,
            F.aggregate(
                F.map_values(F.col(count_map_column)),
                F.lit(0).cast(LongType()),
                lambda a, b: a + b,
            ),
        )
    )

    return estimated_ind_cqc_filled_posts_by_job_role_df


def create_ratios_map_from_count_map_and_total(
    estimated_ind_cqc_filled_posts_by_job_role_df: DataFrame,
    count_map_column: str,
    count_map_values_total_column: str,
    ratio_map_column_name: str,
) -> DataFrame:
    """
    Adds a column which contains a ratio map.

    Takes a map column and a column with the total of values from that map column.
    Makes another map with the same keys as the given map but values as each value in the given map
    as a percentage of total column.

    Args:
        estimated_ind_cqc_filled_posts_by_job_role_df (DataFrame): A dataframe containing a job role count map at workplace level.
        count_map_column (str): A map column.
        count_map_values_total_column (str): A column with the total of values from the count_map_column.
        ratio_map_column_name (str): The name to give to the new ratio map column.

    Returns:
        DataFrame: The estimated filled post by job role DataFrame with the job role ratio map column joined in.
    """

    estimated_ind_cqc_filled_posts_by_job_role_df = (
        estimated_ind_cqc_filled_posts_by_job_role_df.withColumn(
            ratio_map_column_name,
            F.map_from_arrays(
                F.map_keys(F.col(count_map_column)),
                F.transform(
                    F.map_values(F.col(count_map_column)),
                    lambda v: v / F.col(count_map_values_total_column),
                ),
            ),
        )
    )

    return estimated_ind_cqc_filled_posts_by_job_role_df


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
