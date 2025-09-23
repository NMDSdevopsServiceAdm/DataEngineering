import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc.utils.utils.utils import merge_columns_in_order
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def estimate_non_res_capacity_tracker_filled_posts(df: DataFrame) -> DataFrame:
    care_worker_ratio = calculate_care_worker_ratio(df)
    df = convert_to_all_posts_using_ratio(df, care_worker_ratio)
    df = merge_columns_in_order(
        df,
        [
            IndCQC.ct_non_res_all_posts,
            IndCQC.estimate_filled_posts,
        ],
        IndCQC.ct_non_res_filled_post_estimate,
        IndCQC.ct_non_res_filled_post_estimate_source,
    )
    return df


def calculate_care_worker_ratio(df: DataFrame) -> float:
    """
    Calculate the overall ratio of care workers to all posts and print it.

    Args:
        df (DataFrame): A dataframe containing the columns estimate_filled_posts and cqc_care_workers_employed_imputed.
    Returns:
        float: A float representing the ratio between care workers and all posts.
    """
    df = df.where(
        (df[IndCQC.care_home] == CareHome.not_care_home)
        & (df[IndCQC.ct_non_res_care_workers_employed_imputed].isNotNull())
        & (df[IndCQC.estimate_filled_posts].isNotNull())
    )
    total_care_workers = df.agg(
        F.sum(df[IndCQC.ct_non_res_care_workers_employed_imputed])
    ).collect()[0][0]
    total_posts = df.agg(F.sum(df[IndCQC.estimate_filled_posts])).collect()[0][0]
    care_worker_ratio = total_care_workers / total_posts
    logger.info(f"The care worker ratio used is: {care_worker_ratio}.")
    return care_worker_ratio


def convert_to_all_posts_using_ratio(
    df: DataFrame, care_worker_ratio: float
) -> DataFrame:
    """
    Convert the cqc_care_workers_employed figures to all workers.

    Args:
        df (DataFrame): A dataframe with non res capacity tracker data.
        care_worker_ratio (float): The ratio of all care workers divided by all posts.

    Returns:
        DataFrame: A dataframe with a new column containing the all-workers estimate.
    """
    df = df.withColumn(
        IndCQC.ct_non_res_all_posts,
        F.when(
            (df[IndCQC.care_home] == CareHome.not_care_home),
            F.col(IndCQC.ct_non_res_care_workers_employed_imputed) / care_worker_ratio,
        ).otherwise(F.lit(None)),
    )
    return df
