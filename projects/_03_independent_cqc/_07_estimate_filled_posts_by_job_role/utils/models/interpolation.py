from pyspark.sql import DataFrame, functions as F
from typing import List

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.utils.utils import (
    unpack_mapped_column,
    create_map_column,
    pivot_job_role_column,
    convert_map_with_all_null_values_to_null,
)
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models.interpolation import (
    model_interpolation,
)


def model_job_role_ratio_interpolation(
    df: DataFrame, job_role_column_list: List[str]
) -> DataFrame:
    """
    Performs interpolation on ascwds_job_role_ratio_filtered column

    Args:
        df (DataFrame): The input DataFrame containing the columng ascwds_job_role_ratio_filtered column
        job_role_column_list (List[str]): List of all job roles in ASCWDS.

    Returns:
        DataFrame: The DataFrame with the ascwds_job_role_ratio_interpolated column
    """
    df_to_interpolate = unpack_mapped_column(df, IndCQC.ascwds_job_role_ratios_filtered)
    df_to_interpolate = create_map_column(
        df_to_interpolate, job_role_column_list, IndCQC.ascwds_job_role_ratios_temporary
    )

    df_to_interpolate = df_to_interpolate.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        F.explode(IndCQC.ascwds_job_role_ratios_temporary).alias(
            IndCQC.main_job_role_clean_labelled, IndCQC.ascwds_job_role_ratios_exploded
        ),
    )

    df_to_interpolate = model_interpolation(
        df_to_interpolate,
        IndCQC.ascwds_job_role_ratios_exploded,
        "straight",
        IndCQC.ascwds_job_role_ratios_interpolated,
        [IndCQC.location_id, IndCQC.main_job_role_clean_labelled],
    )

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCQC.ascwds_job_role_ratios_interpolated,
        F.coalesce(
            F.col(IndCQC.ascwds_job_role_ratios_interpolated),
            F.col(IndCQC.ascwds_job_role_ratios_exploded),
        ),
    )

    df_to_interpolate = pivot_job_role_column(
        df_to_interpolate,
        [IndCQC.location_id, IndCQC.unix_time],
        IndCQC.ascwds_job_role_ratios_interpolated,
    )

    df_to_interpolate = create_map_column(
        df_to_interpolate,
        job_role_column_list,
        IndCQC.ascwds_job_role_ratios_interpolated,
    )

    df_joined = df.join(
        df_to_interpolate, on=[IndCQC.location_id, IndCQC.unix_time], how="inner"
    )

    df_result = convert_map_with_all_null_values_to_null(df_joined)

    return df_result
