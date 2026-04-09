from typing import Final

import polars as pl

from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils import (
    percentage_share,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Define constants for IDs for original length data and expanded data.
ROW_ID: Final[str] = "id"
EXPANDED_ID: Final[str] = "expanded_id"

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

    # Rename impute_ratios to be more descriptive. E.g. create_imputed_ascwds_job_role_counts or something similar.
    # Add the multiplication below this function into "impute_ratios".
    estimated_job_role_posts_lf = impute_ratios(estimated_job_role_posts_lf)

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


def impute_ratios(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Impute job role ratios by interpolation forward fill and backward fill.

    Uses groupby-agg-explode pattern to keep processing within polars streaming
    engine.
    """
    impute_groups = [IndCQC.location_id, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date

    imputed_ratios = (
        pl.col(IndCQC.ascwds_job_role_ratios)
        .sort_by(order_key)
        .interpolate()
        .forward_fill()
        .backward_fill()
        .alias(IndCQC.imputed_ascwds_job_role_ratios)
    )

    impute_agg_lf = (
        estimated_job_role_posts_lf.group_by(impute_groups)
        .agg(
            # Sort the join key in the same manner as the imputed values.
            pl.col(EXPANDED_ID).sort_by(order_key),
            imputed_ratios,
        )
        .explode(EXPANDED_ID, IndCQC.imputed_ascwds_job_role_ratios)
        .drop(impute_groups)
    )

    return estimated_job_role_posts_lf.join(impute_agg_lf, on=EXPANDED_ID, how="left")


def get_percent_share_ratios(
    estimated_job_role_posts_lf: pl.LazyFrame,
    input_col: str,
    output_col: str,
) -> pl.LazyFrame:
    """Calculate ratios over location and date using groupby-agg-explode pattern.

    Using groupby-agg-explode ensures it can be processed with the streaming engine.
    """
    groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]

    # Groupby-agg-explode on only necessary subset, before joining back on EXPANDED_ID.
    ratios_agg_lf = (
        estimated_job_role_posts_lf.group_by(groups)
        .agg(
            pl.col(EXPANDED_ID),  # Keep to align during explode
            percentage_share(input_col).cast(pl.Float32).alias(output_col),
        )
        .explode(EXPANDED_ID, output_col)
        # Drop groups to prevent duplicate columns after join.
        .drop(groups)
    )

    return estimated_job_role_posts_lf.join(ratios_agg_lf, on=EXPANDED_ID, how="left")


def get_job_counts_rolling_sum(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """ """
    rolling_groups = [IndCQC.primary_service_type, IndCQC.main_job_role_clean_labelled]
    order_key = IndCQC.cqc_location_import_date
    monthly_groups = rolling_groups + [order_key]
    # STEP A: Pre-aggregate down to monthly totals
    # (Shrinks 152M rows -> ~50k rows instantly via Hash Aggregation)
    monthly_totals_lf = estimated_job_role_posts_lf.group_by(monthly_groups).agg(
        pl.col(IndCQC.imputed_ascwds_job_role_counts).sum()
    )

    # STEP B: Sort and roll on the small dataset.
    # This .sort() is completely safe because it's only operating on ~50k rows.
    rolling_agg_lf = (
        monthly_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period="6mo")
        .agg(pl.col(IndCQC.imputed_ascwds_job_role_counts).sum().alias("rolling_sum"))
    )

    # STEP C: Join the rolling sum back to the main 152M row table
    return estimated_job_role_posts_lf.join(
        rolling_agg_lf,
        on=monthly_groups,
        how="left",
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
