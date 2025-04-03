from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from typing import Optional, List

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCqc,
)

from utils.utils import convert_days_to_unix_time

from utils.estimate_filled_posts_by_job_role_utils.utils import (
    create_map_column,
    pivot_mapped_column,
    list_of_job_roles_sorted,
)


def calculate_rolling_sum_of_job_roles(
    df: DataFrame, number_of_days_in_rolling_sum: int
) -> DataFrame:
    """
    Adds a rolling sum of job of job role counts mapped column from the job role counts mapped column

    Args:
        df (DataFrame): The input DataFrame, which has the job role counts mapped column
        number_of_days_in_rolling_sum (int): The number of days to include in the rolling time period which will be used within add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled.

    Returns:
        DataFrame: The DataFrame with the new rolling sum of job role counts mapped column

    """

    df_rolling_sum = df.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        IndCQC.primary_service_type,
        F.explode(IndCQC.ascwds_job_role_counts).alias(
            IndCQC.main_job_role_clean_labelled, IndCQC.ascwds_job_role_counts_exploded
        ),
    )

    df_rolling_sum = add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled(
        df_rolling_sum,
        number_of_days_in_rolling_sum,
        IndCQC.ascwds_job_role_counts_exploded,
        IndCQC.ascwds_job_role_counts_rolling_sum,
    )

    df_rolling_sum = pivot_mapped_column(
        df_rolling_sum,
        [IndCQC.unix_time, IndCQC.primary_service_type],
        IndCQC.ascwds_job_role_counts_rolling_sum,
    )

    existing_columns = [
        col for col in list_of_job_roles_sorted if col in df_rolling_sum.columns
    ]

    df_rolling_sum = create_map_column(
        df_rolling_sum,
        existing_columns,
        IndCQC.ascwds_job_role_counts_rolling_sum,
        True,
    )

    df_result = df.join(
        df_rolling_sum,
        on=[IndCQC.unix_time, IndCQC.primary_service_type],
        how="left",
    )

    return df_result


def add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled(
    df: DataFrame,
    number_of_days: int,
    column_to_sum: str,
    rolling_sum_column_name: str,
) -> DataFrame:
    """
    Adds a rolling sum column to a DataFrame based on a specified number of days, partitioned by primary service type and main job role clean labelled.

    Args:
        df (DataFrame): The input DataFrame.
        number_of_days (int): The number of days to include in the rolling time period.
        column_to_sum (str): The name of the column to sum.
        rolling_sum_column_name (str): The name of the new column to store the rolling sum.

    Returns:
        DataFrame: The DataFrame with the new rolling sum column added.

    """
    rolling_sum_window = (
        Window.partitionBy(
            [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled]
        )
        .orderBy(F.col(IndCqc.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    df = df.withColumn(
        rolling_sum_column_name,
        F.sum(F.col(column_to_sum)).over(rolling_sum_window),
    )
    return df
