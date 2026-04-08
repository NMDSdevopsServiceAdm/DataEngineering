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

# Define constants for IDs for original length data and expanded data.
ROW_ID: Final[str] = "id"

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

manager_roles_list = AscwdsWorkerValueLabelsJobGroup.manager_roles()


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

    lf = adjust_managerial_filled_posts(lf)

    # Try to refactor the pipe(apply_manager_adjustments) so it's readable sequence of expressions without class methods.
    #       estimated_job_role_posts_lf = estimated_job_role_posts_lf.pipe(
    #           apply_manager_adjustments
    #       )

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


def adjust_managerial_filled_posts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Runs the steps required to adjust managerial filled post estimates to
    account for replacing SfC registered manager estimate with CQC registered
    manager count.

    The steps are:
        1. Separate non-managerial role rows into a temp LazyFrame.
        2. Filter the input LazyFrame to managerial role rows only and pivot
           filled posts for each role into a column.
        3. Calculate the adjusted managerial role filled posts.
        4. Unpivot the adjusted managerial role filled post columns into rows.
        5. Concatenate the adjusted managerial role rows and non-managerial role
           rows.
    """
    non_managerial_roles_lf = lf.filter(
        pl.col(IndCQC.main_job_role_clean_labelled) != manager_roles_list
    ).drop(IndCQC.registered_manager_count)

    managerial_roles_lf = filter_rows_and_pivot_into_columns(lf, manager_roles_list)

    managerial_roles_lf = recalculate_managerial_filled_posts(
        managerial_roles_lf, manager_roles_list
    )

    managerial_roles_lf = unpivot_job_roles_into_rows(managerial_roles_lf)

    return pl.concat([non_managerial_roles_lf, managerial_roles_lf])


def filter_rows_and_pivot_into_columns(
    lf: pl.LazyFrame, list_of_roles: list
) -> pl.LazyFrame:
    """
    Filters LazyFrame to given list of job roles rows and pivots their filled
    posts values into a column per role.

    Args:
        lf(pl.LazyFrame): The input LazyFrame.
        list_of_roles (list): A list of job roles to create columns from their rows.

    Returns:
        pl.LazyFrame: The input LazyFrame with manager roles only and pivoted on
        job role.
    """
    return lf.pivot(
        on=IndCQC.main_job_role_clean_labelled,
        on_columns=list_of_roles,
        index=[ROW_ID, IndCQC.registered_manager_count],
        values=IndCQC.estimate_filled_posts_by_job_role,
        aggregate_function="first",
    )


def recalculate_managerial_filled_posts(
    lf: pl.LazyFrame, list_of_roles: list
) -> pl.LazyFrame:
    """
    Doc string goes here
    """
    non_rm_managerial_roles_list = [
        role for role in list_of_roles if role != MainJobRoleLabels.registered_manager
    ]

    manager_diff = pl.col(MainJobRoleLabels.registered_manager) - pl.col(
        IndCQC.registered_manager_count
    )

    non_rm_managerial_roles_sum = pl.sum_horizontal(non_rm_managerial_roles_list)

    number_of_non_rm_managerial_roles = len(non_rm_managerial_roles_list)

    return lf.select(
        ROW_ID,
        *[
            pl.when(non_rm_managerial_roles_sum.eq(0))
            .then(
                pl.col(col).add(
                    manager_diff * (pl.lit(1 / number_of_non_rm_managerial_roles))
                )
            )
            .otherwise(
                pl.col(col).add(
                    manager_diff * (pl.col(col) / non_rm_managerial_roles_sum)
                )
            )
            .cast(pl.Float32)
            .clip(lower_bound=0.0)
            .name.keep()
            for col in non_rm_managerial_roles_list
        ],
        pl.col(IndCQC.registered_manager_count)
        .alias(MainJobRoleLabels.registered_manager)
        .cast(pl.Float32),
    )


def unpivot_job_roles_into_rows(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Doc string goes here.
    """
    lf = lf.unpivot(
        on=manager_roles_list,
        index=ROW_ID,
        variable_name=IndCQC.main_job_role_clean_labelled,
        value_name=IndCQC.estimate_filled_posts_by_job_role,
    )

    lf = lf.with_columns(
        pl.col(IndCQC.main_job_role_clean_labelled).cast(
            pl.Enum(AscwdsWorkerValueLabelsJobGroup.all_roles())
        )
    )

    return lf


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
