from typing import Final

import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    ManagerialFilledPostAdjustmentExpr,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Define constants for IDs for original length data.
ROW_ID: Final[str] = "id"

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)


def main(
    imputed_data_source: str,
    metadata_source: str,
    estimated_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        imputed_data_source (str): path to the imputed data
        metadata_source (str): path to the metadata created in clean step
        estimated_data_destination (str): destination for output
    """
    print("Estimates Job Started...")

    lf = utils.scan_parquet(imputed_data_source)
    print("Imputed LazyFrame read in")

    #       metadata_lf = utils.scan_parquet(metadata_source)
    #       print("Metadata LazyFrame read in")

    # Abstract this into a defined function. So that main is calling a series of steps.
    lf = calculate_estimated_filled_posts_by_job_role(lf)
    print("Calculated estimates by job role")

    lf = lf.with_columns(count_cqc_rm().alias(IndCQC.registered_manager_count))
    print("Calculated registered manager count")

    lf = adjust_managerial_roles(lf)
    print("Adjusted managerial roles")

    # Join back original metadata.
    #       estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
    #           metadata_lf,
    #           on="id",
    #       )

    utils.sink_to_parquet(
        lazy_df=lf,
        output_path=estimated_data_destination,
        append=False,
    )


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
            pl.col(IndCQC.estimate_filled_posts)
            * pl.col(IndCQC.ascwds_job_role_ratios_merged)
        ).alias(IndCQC.estimate_filled_posts_by_job_role)
    )

    return lf


def count_cqc_rm() -> pl.Expr:
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


def adjust_managerial_roles(lf: pl.LazyFrame) -> pl.LazyFrame:
    """ "
    A function to call steps for adjusting managerial roles.
    """
    # lf = lf.filter(
    #     pl.col(IndCQC.main_job_role_clean_labelled).is_in(manager_roles_list)
    # )

    manager_roles = AscwdsWorkerValueLabelsJobGroup.manager_roles()
    non_rm_manager_roles = [
        role for role in manager_roles if role != MainJobRoleLabels.registered_manager
    ]
    non_rm_manager_condition = pl.col(IndCQC.main_job_role_clean_labelled).is_in(
        non_rm_manager_roles
    )

    lf = get_reg_man_difference(lf)
    lf = get_non_rm_managerial_distribution(lf, non_rm_manager_condition)
    lf = redistribute_rm_difference(lf, non_rm_manager_condition)

    return lf


def get_reg_man_difference(lf: pl.LazyFrame) -> pl.LazyFrame:

    return lf.with_columns(
        (
            (
                pl.col(IndCQC.estimate_filled_posts_by_job_role)
                - pl.col(IndCQC.registered_manager_count)
            )
            .filter(
                pl.col(IndCQC.main_job_role_clean_labelled)
                == MainJobRoleLabels.registered_manager
            )
            .first(ignore_nulls=True)
            .over(ROW_ID)
        ).alias(IndCQC.difference_between_estimate_and_cqc_registered_managers)
    )


def get_non_rm_managerial_distribution(
    lf: pl.LazyFrame, non_rm_manager_condition: pl.Expr
) -> pl.LazyFrame:
    """
    Doc string goes here.
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
            pl.when(non_rm_manager_condition).then(
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
        ).alias(IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role)
    )

    return lf


def redistribute_rm_difference(
    lf: pl.LazyFrame, non_rm_manager_condition: pl.Expr
) -> pl.LazyFrame:
    """
    Doc string here
    """
    redistribution_expr = pl.col(IndCQC.estimate_filled_posts_by_job_role).add(
        pl.col(IndCQC.difference_between_estimate_and_cqc_registered_managers).mul(
            IndCQC.proportion_of_non_rm_managerial_estimated_filled_posts_by_role
        )
    )

    return lf.with_columns(
        pl.when(non_rm_manager_condition)
        .then(
            pl.when(redistribution_expr < 0)
            .then(IndCQC.estimate_filled_posts_by_job_role)
            .otherwise(redistribution_expr)
        )
        .when(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == MainJobRoleLabels.registered_manager
        )
        .then(pl.col(IndCQC.registered_manager_count).cast(pl.Float32))
        .otherwise(pl.col(IndCQC.estimate_filled_posts_by_job_role))
        .alias(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
    )


# def apply_manager_adjustments(
#     estimated_job_role_posts_lf: pl.LazyFrame,
# ) -> pl.LazyFrame:
#     """Apply the managerial adjustments."""
#     pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
#     is_non_rm_manager = ManagerialFilledPostAdjustmentExpr._is_non_rm_manager()
#     filled_posts = pl.col(IndCQC.estimate_filled_posts_by_job_role)

#     stats_lf = estimated_job_role_posts_lf.group_by(pct_share_groups).agg(
#         rm_diff=ManagerialFilledPostAdjustmentExpr._rm_manager_diff(),
#         non_rm_total=(pl.when(is_non_rm_manager).then(filled_posts).sum()),
#         non_rm_len=(pl.when(is_non_rm_manager).then(pl.lit(1)).sum()),
#     )

#     estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
#         stats_lf,
#         on=pct_share_groups,
#         how="left",
#     )

#     return (
#         estimated_job_role_posts_lf.with_columns(
#             pct_share=pl.when(is_non_rm_manager).then(
#                 pl.when(pl.col("non_rm_total") == 0)
#                 .then(1 / pl.col("non_rm_len"))
#                 .otherwise(filled_posts / pl.col("non_rm_total"))
#             )
#         )
#         .with_columns(
#             pl.when(is_non_rm_manager)
#             .then(
#                 filled_posts.add(pl.col("rm_diff").mul(pl.col("pct_share"))).clip(
#                     lower_bound=0
#                 )
#             )
#             .when(ManagerialFilledPostAdjustmentExpr._is_registered_manager)
#             .then(ManagerialFilledPostAdjustmentExpr._clip_rm_count())
#             .otherwise(filled_posts)
#             .alias(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
#         )
#         .drop("pct_share", "rm_diff", "non_rm_total", "non_rm_len")
#     )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--imputed_data_source",
            "Source s3 directory for imputed data",
        ),
        ("--metadata_source", "Destination s3 directory for estimates by job role"),
        (
            "--estimated_data_destination",
            "Destination s3 directory for estimates by job role",
        ),
    )
    main(
        imputed_data_source=args.imputed_data_source,
        metadata_source=args.metadata_source,
        estimated_data_destination=args.estimated_data_destination,
    )
