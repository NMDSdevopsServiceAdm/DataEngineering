from pyspark.sql import DataFrame, functions as F, Window
from typing import Optional, Tuple

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.estimate_filled_posts_by_job_role_utils.utils import (
    unpack_mapped_column,
    create_map_column,
    pivot_interpolated_job_role_ratios,
)

import utils.estimate_filled_posts.models.interpolation as interpolation


def model_job_role_ratio_interpolation(
    df: DataFrame,
) -> DataFrame:
    """
    Performs interpolation on ascwds_job_role_ratio column

    Args:
        df (DataFrame): The input DataFrame containing the columng ascwds_job_role_ratio column

    Returns:
        DataFrame: The DataFrame with the ascwds_job_role_ratio_interpolated column

    """

    df_to_interpolate = unpack_mapped_column(df, IndCQC.ascwds_job_role_ratios)

    df_keys = df_to_interpolate.select(
        F.explode(F.map_keys(F.col(IndCQC.ascwds_job_role_ratios)))
    ).distinct()
    columns_to_interpolate = sorted([row[0] for row in df_keys.collect()])

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCQC.ascwds_job_role_ratios_temporary,
        create_map_column(columns_to_interpolate),
    )

    df_to_interpolate = df_to_interpolate.drop(*columns_to_interpolate)

    df_to_interpolate = df_to_interpolate.select(
        IndCQC.location_id,
        IndCQC.unix_time,
        F.explode(IndCQC.ascwds_job_role_ratios_temporary).alias(
            IndCQC.main_job_role_clean_labelled, "ratios"
        ),
    )

    df_to_interpolate = interpolation.model_interpolation(
        df_to_interpolate,
        "ratios",
        "straight",
        IndCQC.ascwds_job_role_ratios_interpolated,
        [IndCQC.location_id, IndCQC.main_job_role_clean_labelled],
    )

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCQC.ascwds_job_role_ratios_interpolated,
        F.coalesce(F.col(IndCQC.ascwds_job_role_ratios_interpolated), F.col("ratios")),
    )

    df_to_interpolate.show(truncate=False)

    df_to_interpolate = pivot_interpolated_job_role_ratios(df_to_interpolate)

    df_to_interpolate.show(truncate=False)

    df_to_interpolate = df_to_interpolate.withColumn(
        IndCQC.ascwds_job_role_ratios_interpolated,
        create_map_column(columns_to_interpolate),
    )

    df_to_interpolate = df_to_interpolate.drop(*columns_to_interpolate)

    df_result = df_to_interpolate.join(
        df, on=[IndCQC.location_id, IndCQC.unix_time], how="inner"
    )

    return df_result
