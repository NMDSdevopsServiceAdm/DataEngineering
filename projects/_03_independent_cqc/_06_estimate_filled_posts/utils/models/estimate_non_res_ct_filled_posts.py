import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc.utils.utils.utils import merge_columns_in_order
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def estimate_non_res_capacity_tracker_filled_posts(df: DataFrame) -> DataFrame:
    """
    Estimate the total number of filled posts for non-residential care providers in the capacity tracker.

    Non-residential care locations only record the number of care workers they employ, not the total number of staff.
    This function estimates the total number of filled posts by calculating the ratio of care workers to Skills for
    Care estimated total posts, and then applies this ratio to uplift the care worker figures to estimate all staff.

    A new column is created containing the filled post estimate, along with a source column indicating whether the
    estimate was:
        - derived from the capacity tracker data, or
        - if they've never submitted capacity trackers, the Skills for Care estimate.

    Args:
        df (DataFrame): A dataframe with non res capacity tracker data.

    Returns:
        DataFrame: A dataframe with a new column containing the filled post estimate and the source of the estimate.
    """
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
    Calculate the overall ratio of Capacity Tracker (DT) care workers to all estimated filled posts.

    Non-residential care locations only record the number of care workers they employ.
    This function calculates the ratio of CT care workers to Skills for Care (SfC) estimated total posts.

    Args:
        df (DataFrame): A dataframe containing the Capacity Tracker care workers and SfC estimated filled posts.

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
    Convert the Capacity Tracker care worker only figures to estimate all job rob roles.

    Args:
        df (DataFrame): A dataframe containing non residential Capacity Tracker data.
        care_worker_ratio (float): The ratio to convert care workers to represent all job roles.

    Returns:
        DataFrame: A dataframe with a new column containing the estimate of all filled posts.
    """
    df = df.withColumn(
        IndCQC.ct_non_res_all_posts,
        F.when(
            (df[IndCQC.care_home] == CareHome.not_care_home),
            F.col(IndCQC.ct_non_res_care_workers_employed_imputed) / care_worker_ratio,
        ).otherwise(F.lit(None)),
    )
    return df
