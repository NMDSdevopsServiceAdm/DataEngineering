import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


def estimate_non_res_capacity_tracker_filled_posts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Estimate the total number of filled posts for non-residential care providers
    in the capacity tracker.

    Non-residential care locations only record the number of care workers they
    employ, not the total number of staff. This function estimates the total
    number of filled posts by calculating the ratio of care workers to Skills
    for Care estimated total posts, and then applies this ratio to uplift the
    care worker figures to estimate all staff.

    A new column is created containing the filled post estimate, along with a
    source column indicating whether the estimate was:
        - derived from the capacity tracker data, or
        - if they've never submitted capacity trackers, the Skills for Care
          estimate.

    Args:
        lf (pl.LazyFrame): A LazyFrame with non res capacity tracker data.

    Returns:
        pl.LazyFrame: A LazyFrame with a new column containing the filled post
        estimate and the source of the estimate.
    """
    care_worker_ratio = calculate_care_worker_ratio()

    lf = lf.with_columns(
        (
            pl.when(pl.col(IndCQC.care_home) == CareHome.not_care_home)
            .then(
                pl.col(IndCQC.ct_non_res_care_workers_employed_imputed).truediv(
                    care_worker_ratio
                )
            )
            .alias(IndCQC.ct_non_res_all_posts)
        ),
        (
            utils.coalesce_with_source_labels(
                cols=[
                    IndCQC.ct_non_res_all_posts,
                    IndCQC.estimate_filled_posts,
                ],
                name=IndCQC.ct_non_res_filled_post_estimate,
            )
        ),
        (
            pl.col(IndCQC.ct_non_res_filled_post_estimate)
            .clip(lower_bound=1.0)
            .alias(IndCQC.ct_non_res_filled_post_estimate)
        ),
        (
            utils.nullify_ct_values_previous_to_first_submission(
                [
                    IndCQC.ct_non_res_filled_post_estimate,
                    IndCQC.ct_non_res_filled_post_estimate_source,
                ]
            )
        ),
    )

    return lf


def calculate_care_worker_ratio() -> pl.Expr:
    """
    Calculate the overall ratio of Capacity Tracker care workers
    to all estimated filled posts at non-residential services.

    Non-residential care locations only record the number of care workers they
    employ. This function calculates the ratio of CT care workers to Skills for
    Care (SfC) estimated filled posts.

    Returns:
        pl.Expr: A Polars expression to calculate the ratio between care workers
        and all filled posts at non-residential services.
    """
    ratio_filter = (
        (pl.col(IndCQC.care_home) == CareHome.not_care_home)
        & (pl.col(IndCQC.ct_non_res_care_workers_employed_imputed).is_not_null())
        & (pl.col(IndCQC.estimate_filled_posts).is_not_null())
    )
    sum_ct_care_workers = (
        pl.col(IndCQC.ct_non_res_care_workers_employed_imputed)
        .filter(ratio_filter)
        .sum()
    )
    sum_sfc_filled_posts = (
        pl.col(IndCQC.estimate_filled_posts).filter(ratio_filter).sum()
    )
    care_worker_ratio = sum_ct_care_workers.truediv(sum_sfc_filled_posts)
    print(f"The care worker ratio used is: {care_worker_ratio}.")

    return care_worker_ratio
