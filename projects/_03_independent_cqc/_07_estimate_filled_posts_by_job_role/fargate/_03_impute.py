import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    create_imputed_ascwds_job_role_counts,
    get_job_counts_rolling_sum,
    get_percent_share_ratios,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Creates estimates of filled posts split by main job role.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """

    print("Imputing Cleaned dataset...")

    estimated_job_role_posts_lf = utils.scan_parquet(cleaned_data_source)
    print("Cleaned LazyFrame read in")

    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=IndCQC.ascwds_job_role_counts,
        output_col=IndCQC.ascwds_job_role_ratios,
    )

    # Add the multiplication below this function into "create_imputed_ascwds_job_role_counts".
    estimated_job_role_posts_lf = create_imputed_ascwds_job_role_counts(
        estimated_job_role_posts_lf
    )

    # Multiply imputed ratios by estimate filled posts
    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.col(IndCQC.estimate_filled_posts)
        .mul(pl.col(IndCQC.imputed_ascwds_job_role_ratios))
        .alias(IndCQC.imputed_ascwds_job_role_counts)
    )

    # Combine the count rolling sum and get_percent_share_ratio into one function that returns ascwds_job_role_rolling_ratio.
    estimated_job_role_posts_lf = get_job_counts_rolling_sum(
        estimated_job_role_posts_lf
    )
    estimated_job_role_posts_lf = get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col="rolling_sum",
        output_col=IndCQC.ascwds_job_role_rolling_ratio,
    )

    utils.sink_to_parquet(
        lazy_df=estimated_job_role_posts_lf,
        output_path=imputed_data_destination,
        append=False,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
        ),
        ("--imputed_data_destination", "Destination s3 directory for imputed data"),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        imputed_data_destination=args.imputed_data_destination,
    )
