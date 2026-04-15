from typing import Final

import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import MainJobRoleLabels

# Define constants for IDs for original length data.
ROW_ID: Final[str] = "id"


def calculate_estimated_filled_posts_by_job_role(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Coalesce the imputed and rolling average job role ratio columns in
    that order to create ascwds_job_role_ratios_merged and multiply that by
    overall estimated filled posts.

    TODO: uncomment ascwds_job_role_ratios_filtered and update this doc string.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.

    Returns:
        pl.LazyFrame: The input LazyFrame with additional columns
            ascwds_job_role_ratios_merged and estimate_filled_posts_by_job_role
    """
    lf = lf.with_columns(
        utils.coalesce_with_source_labels(
            cols=[
                # IndCQC.ascwds_job_role_ratios_filtered,
                IndCQC.imputed_ascwds_job_role_ratios,
                IndCQC.ascwds_job_role_rolling_ratio,
            ],
            name=IndCQC.ascwds_job_role_ratios_merged,
        ),
    )

    lf = lf.with_columns(
        (
            pl.col(IndCQC.estimate_filled_posts).mul(
                pl.col(IndCQC.ascwds_job_role_ratios_merged)
            )
        ).alias(IndCQC.estimate_filled_posts_by_job_role)
    )

    return lf


def has_rm_in_cqc_rm_name_list_flag() -> pl.Expr:
    """
    Returns an expression that produces 1 where list of register
    manager names is >= 1, otherwise 0.

    Returns:
        pl.Expr: Expression that produces register manager count.
    """
    return (
        pl.col(IndCQC.registered_manager_names)
        .list.len()
        .clip(upper_bound=1)
        .fill_null(0)
    )


def adjust_managerial_roles(
    lf: pl.LazyFrame, non_rm_manager_roles: pl.List
) -> pl.LazyFrame:
    """
    A function that calls steps for adjusting managerial roles to account for
    replacing SfC registered manager estimate with registered manager count from
    CQC.

    The steps are:
        1. Get the difference between SfC estimate and CQC count.
        2. Get the precentage split of non registered manager managerial role
           estimates.
        3. Multiply the difference by the percentage split and add it the
           estimate, and replace SfC registered manager estimate with CQC count.

    Args:
        lf (pl.LazyFrame): A LazyFrame with estimated filled posts by job role
            and registered manager count from CQC.
        non_rm_manager_roles (pl.List): A list of non registered manager
            managerial job roles.

    Returns:
        pl.LazyFrame: The input LazyFrame with column
            'estimate_filled_posts_by_job_role_manager_adjusted'.
    """
    non_rm_manager_condition = pl.col(IndCQC.main_job_role_clean_labelled).is_in(
        non_rm_manager_roles
    )

    lf = calculate_reg_man_difference(lf)
    lf = calculate_non_rm_managerial_distribution(lf, non_rm_manager_condition)
    lf = distribute_rm_difference(lf, non_rm_manager_condition)

    return lf.drop(
        [
            IndCQC.registered_manager_count,
            IndCQC.difference_between_estimate_and_cqc_registered_managers,
            IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role,
        ]
    )


def calculate_reg_man_difference(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculates the difference between SfC estimate of registered manager filled
    posts and count of registered managers from CQC.

    The difference is calculated on registered manager rows, and the result is
    copied to all rows, per locationid/import_date ("id").

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'estimate_filled_posts_by_job_role' and 'registered_manager_count'

    Returns:
        pl.LazyFrame: The input LazyFrame with column
            'difference_between_estimate_and_cqc_registered_managers'.
    """
    return lf.with_columns(
        (
            (
                pl.col(IndCQC.estimate_filled_posts_by_job_role).sub(
                    pl.col(IndCQC.registered_manager_count)
                )
            )
            .filter(
                pl.col(IndCQC.main_job_role_clean_labelled)
                == MainJobRoleLabels.registered_manager
            )
            .first(ignore_nulls=True)
            .over(ROW_ID)
        ).alias(IndCQC.difference_between_estimate_and_cqc_registered_managers)
    )


def calculate_non_rm_managerial_distribution(
    lf: pl.LazyFrame, non_rm_manager_condition: pl.Expr
) -> pl.LazyFrame:
    """
    Calculates the percentage distribution of filled post for managerial roles
    that are not registered managers. If the sum of non-rm managerial filled
    posts is zero then result is 1 / number of non-rm managerial roles.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'estimate_filled_posts_by_job_role'
        non_rm_manager_condition (pl.Expr): Expression that is True if job role is
            managerial but not a registered manager.

    Returns:
        pl.LazyFrame: The input LazyFrame with column
            'proportion_of_non_rm_managerial_estimated_filled_posts_by_role'.
    """
    sum_non_rm_managerial_posts_expr = (
        pl.col(IndCQC.estimate_filled_posts_by_job_role)
        .filter(non_rm_manager_condition)
        .sum()
        .over(ROW_ID)
    )

    count_non_rm_managerial_roles_expr = (
        pl.lit(1).filter(non_rm_manager_condition).sum().over(ROW_ID)
    )

    lf = lf.with_columns(
        (
            pl.when(non_rm_manager_condition)
            .then(
                pl.when(sum_non_rm_managerial_posts_expr.eq(0.0))
                .then(
                    pl.lit(1)
                    .truediv(count_non_rm_managerial_roles_expr)
                    .cast(pl.Float32)
                )
                .otherwise(
                    pl.col(IndCQC.estimate_filled_posts_by_job_role)
                    .truediv(sum_non_rm_managerial_posts_expr)
                    .cast(pl.Float32)
                )
            )
            .otherwise(None)
        ).alias(IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role)
    )

    return lf


def distribute_rm_difference(
    lf: pl.LazyFrame, non_rm_manager_condition: pl.Expr
) -> pl.LazyFrame:
    """
    Distributes the registered manager difference amongst non-rm managerial
    roles and replace SfC registered manager filled posts estimate with
    registered manager count from CQC.

    If the distribution results in filled posts becoming less than zero, then
    the original filled posts value is given instead.

    Args:
        lf (pl.LazyFrame): A LazyFrame with columns
            'estimate_filled_posts_by_job_role',
            'difference_between_estimate_and_cqc_registered_managers',
            'proportion_of_non_rm_managerial_estimated_filled_posts_by_role' and
            'registered_manager_count'.
        non_rm_manager_condition (pl.Expr): Expression that is True if job role is
            managerial but not a registered manager.

    Returns:
        pl.LazyFrame: The input LazyFrame with column
            'estimate_filled_posts_by_job_role_manager_adjusted'.
    """
    redistribution_expr = pl.col(IndCQC.estimate_filled_posts_by_job_role).add(
        pl.col(IndCQC.difference_between_estimate_and_cqc_registered_managers).mul(
            pl.col(
                IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
            )
        )
    )

    return lf.with_columns(
        pl.when(non_rm_manager_condition)
        .then(redistribution_expr.clip(lower_bound=0.0))
        .when(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == MainJobRoleLabels.registered_manager
        )
        .then(pl.col(IndCQC.registered_manager_count).cast(pl.Float32))
        .otherwise(pl.col(IndCQC.estimate_filled_posts_by_job_role))
        .alias(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
    )
