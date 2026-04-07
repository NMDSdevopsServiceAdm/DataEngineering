import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    ManagerialFilledPostAdjustmentExpr,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

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

    estimated_job_role_posts_lf = utils.scan_parquet(imputed_data_source)
    print("Imputed LazyFrame read in")

    metadata_lf = utils.scan_parquet(metadata_source)
    print("Metadata LazyFrame read in")

    # Abstract this into a defined function. So that main is calling a series of steps.
    lf = calculate_estimated_filled_posts_by_job_role(lf)

    # Try to refactor the pipe(apply_manager_adjustments) so it's readable sequence of expressions without class methods.

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.pipe(
        apply_manager_adjustments
    )

    # Join back original metadata.
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        metadata_lf,
        on="id",
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
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


def apply_manager_adjustments(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Apply the managerial adjustments."""
    pct_share_groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
    is_non_rm_manager = ManagerialFilledPostAdjustmentExpr._is_non_rm_manager()
    filled_posts = pl.col(IndCQC.estimate_filled_posts_by_job_role)

    stats_lf = estimated_job_role_posts_lf.group_by(pct_share_groups).agg(
        rm_diff=ManagerialFilledPostAdjustmentExpr._rm_manager_diff(),
        non_rm_total=(pl.when(is_non_rm_manager).then(filled_posts).sum()),
        non_rm_len=(pl.when(is_non_rm_manager).then(pl.lit(1)).sum()),
    )

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.join(
        stats_lf,
        on=pct_share_groups,
        how="left",
    )

    return (
        estimated_job_role_posts_lf.with_columns(
            pct_share=pl.when(is_non_rm_manager).then(
                pl.when(pl.col("non_rm_total") == 0)
                .then(1 / pl.col("non_rm_len"))
                .otherwise(filled_posts / pl.col("non_rm_total"))
            )
        )
        .with_columns(
            pl.when(is_non_rm_manager)
            .then(
                filled_posts.add(pl.col("rm_diff").mul(pl.col("pct_share"))).clip(
                    lower_bound=0
                )
            )
            .when(ManagerialFilledPostAdjustmentExpr._is_registered_manager)
            .then(ManagerialFilledPostAdjustmentExpr._clip_rm_count())
            .otherwise(filled_posts)
            .alias(IndCQC.estimate_filled_posts_by_job_role_manager_adjusted)
        )
        .drop("pct_share", "rm_diff", "non_rm_total", "non_rm_len")
    )


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
