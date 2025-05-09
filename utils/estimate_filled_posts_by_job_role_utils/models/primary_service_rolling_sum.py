from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts_by_job_role_utils.utils import (
    create_map_column,
    pivot_job_role_column,
)
from utils.utils import convert_days_to_unix_time


def calculate_rolling_sum_of_job_roles(
    df: DataFrame, number_of_days_in_rolling_sum: int, list_of_job_roles: list
) -> DataFrame:
    """
    Adds a map column showing the rolling sum of imputed job role counts.

    Args:
        df (DataFrame): A dataFrame containing the 'imputed_ascwds_job_role_counts' map column
        number_of_days_in_rolling_sum (int): The number of days to include in the rolling time period.
        list_of_job_roles (list): A list containing the ASC-WDS job roles.

    Returns:
        DataFrame: A dataFrame with a rolling sum of imputed job role counts map column.
    """

    df_rolling_sum = df.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        IndCQC.primary_service_type,
        F.explode(IndCQC.imputed_ascwds_job_role_counts).alias(
            IndCQC.main_job_role_clean_labelled, IndCQC.ascwds_job_role_counts_exploded
        ),
    )

    df_rolling_sum = add_rolling_sum_partitioned_by_primary_service_type_and_main_job_role_clean_labelled(
        df_rolling_sum,
        number_of_days_in_rolling_sum,
        IndCQC.ascwds_job_role_counts_exploded,
        IndCQC.ascwds_job_role_rolling_sum,
    )

    df_rolling_sum = pivot_job_role_column(
        df_rolling_sum,
        [IndCQC.unix_time, IndCQC.primary_service_type],
        IndCQC.ascwds_job_role_rolling_sum,
    )

    df_rolling_sum = create_map_column(
        df_rolling_sum,
        list_of_job_roles,
        IndCQC.ascwds_job_role_rolling_sum,
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
        .orderBy(F.col(IndCQC.unix_time))
        .rangeBetween(-convert_days_to_unix_time(number_of_days), 0)
    )

    df = df.withColumn(
        rolling_sum_column_name,
        F.sum(F.col(column_to_sum)).over(rolling_sum_window),
    )
    return df
