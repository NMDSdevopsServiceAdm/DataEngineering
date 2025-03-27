from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from typing import Optional, List

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)

from utils.utils import convert_days_to_unix_time

from utils.estimate_filled_posts_by_job_role_utils.utils import (
    unpack_mapped_column,
    create_map_column,
    pivot_rolling_sum_job_role_counts,
)


def calculate_rolling_sum_of_job_roles(df: DataFrame) -> DataFrame:
    """
    Adds a rolling sum of job of job role counts mapped column from the job role counts mapped column

    Args:
        df (DataFrame): The input DataFrame, which has the job role counts mapped column

    Returns:
        DataFrame: The DataFrame with the new rolling sum of job role counts mapped column

    """

    df_rolling_sum = unpack_mapped_column(df, IndCQC.ascwds_job_role_counts)

    df_keys = df_rolling_sum.select(
        F.explode(F.map_keys(F.col(IndCQC.ascwds_job_role_counts)))
    ).distinct()

    job_roles_list = sorted([row[0] for row in df_keys.collect()])

    df_rolling_sum = df_rolling_sum.withColumn(
        IndCQC.ascwds_job_role_counts_temporary,
        create_map_column(job_roles_list),
    )

    df_rolling_sum = df_rolling_sum.drop(*job_roles_list)

    df_rolling_sum = df_rolling_sum.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        IndCQC.primary_service_type,
        F.explode(IndCQC.ascwds_job_role_counts_temporary).alias(
            IndCQC.main_job_role_clean_labelled, IndCQC.ascwds_job_role_counts_exploded
        ),
    )

    df_rolling_sum = add_rolling_sum_partitioned_by_primary_service_type(
        df_rolling_sum,
        185,
        IndCQC.ascwds_job_role_counts_exploded,
        IndCQC.ascwds_job_role_counts_rolling_sum,
        [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled],
    )

    df_rolling_sum = pivot_rolling_sum_job_role_counts(df_rolling_sum)

    df_rolling_sum = df_rolling_sum.withColumn(
        IndCQC.ascwds_job_role_counts_rolling_sum,
        create_map_column(job_roles_list),
    )

    df_rolling_sum = df_rolling_sum.drop(*job_roles_list)

    df_result = df.join(
        df_rolling_sum,
        on=[IndCQC.location_id, IndCQC.unix_time, IndCQC.primary_service_type],
        how="left",
    )

    return df_result


def add_rolling_sum_partitioned_by_primary_service_type(
    df: DataFrame,
    number_of_days: int,
    column_to_sum: str,
    rolling_sum_column_name: str,
    partition_columns: Optional[List[str]] = [IndCqc.primary_service_type],
) -> DataFrame:
    """
    Adds a rolling sum column to a DataFrame based on a specified number of days.

    Adds a rolling sum column to a DataFrame based on a specified number of days.

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling time period.
        column_to_sum (str): The name of the column to sum.
        rolling_sum_column_name (str): The name of the new column to store the rolling sum.
        partition_columns (Optional[List[str]]): The columns we partition by in the window specific which defaults to IndCQC.primary_service_type if empty

    Returns:
        DataFrame: The DataFrame with the new rolling sum column added.

    """
    rolling_sum_window = (
        Window.partitionBy(partition_columns)
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    df = df.withColumn(
        rolling_sum_column_name,
        F.sum(F.col(column_to_sum)).over(rolling_sum_window),
    )
    return df
