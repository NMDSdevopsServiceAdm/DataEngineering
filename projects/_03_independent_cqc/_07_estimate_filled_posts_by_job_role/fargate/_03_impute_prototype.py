"""THROWAWAY. Instrumented copy of _03_impute for one bias investigation.

Mirrors _03_impute.main, but alongside the real pipeline's clamped-and-renormalised
imputed ratio, also computes a shadow ratio using the same trend-following formula with
the floor-at-zero clip and the cross-job-role renormalisation left out. The open question
is how much the floor distorts the sector-level job role split when a trend moves
downward and a chunk of the contributing workplaces are already at zero for that role —
comparing the two aggregated trajectories puts a number on it.

Writes a small, pre-aggregated comparison table (one row per primary service type, size
group, job role and month) to its own dataset name, so it can never overwrite the real
pipeline's output. Runs as its own parallel branch alongside the real impute step in
Ind-CQC-Filled-Post-Estimates-By-Role.json, reusing that task's existing image, IAM role
and security group rather than standing up a separate task definition. Delete this file,
its Dockerfile COPY line and that branch once the investigation concludes.
"""

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.impute_utils as iUtils
from polars_utils import utils
from projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate._03_impute import (
    NumericalValues,
)
from projects._03_independent_cqc.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

SHADOW_UNCLAMPED_RATIO = "shadow_unclamped_ratio"

BIAS_COMPARISON_GROUPS = [
    IndCQC.primary_service_type,
    IndCQC.estimate_filled_posts_size_group,
    IndCQC.main_job_role_clean_labelled,
    IndCQC.cqc_location_import_date,
]


def add_shadow_unclamped_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Add a shadow ratio using the real pipeline's trend-following formula, with the
    floor-at-zero clip and the cross-job-role renormalisation that follow it in
    `add_imputed_ascwds_job_role_ratios` left out.

    Runs extrapolation and interpolation a second time rather than reusing the real
    pipeline's intermediate columns, so this stays fully decoupled from production code —
    it only reads `ascwds_job_role_ratios` and `ascwds_job_role_rolling_ratio`, neither of
    which `add_imputed_ascwds_job_role_ratios` mutates.

    Args:
        estimated_job_role_posts_lf (pl.LazyFrame): dataset already containing
            `ascwds_job_role_ratios` and the `ascwds_job_role_rolling_ratio` trendline.

    Returns:
        pl.LazyFrame: dataset with the added `shadow_unclamped_ratio` column.
    """
    estimated_job_role_posts_lf = model_extrapolation(
        estimated_job_role_posts_lf,
        column_with_null_values=IndCQC.ascwds_job_role_ratios,
        model_to_extrapolate_from=IndCQC.ascwds_job_role_rolling_ratio,
        extrapolation_method="nominal",
        group_columns=iUtils.JOB_ROLE_GROUPS,
    )
    estimated_job_role_posts_lf = model_interpolation(
        estimated_job_role_posts_lf,
        column_with_null_values=IndCQC.ascwds_job_role_ratios,
        method="trend",
        group_columns=iUtils.JOB_ROLE_GROUPS,
    )
    return estimated_job_role_posts_lf.with_columns(
        pl.coalesce(
            IndCQC.ascwds_job_role_ratios,
            IndCQC.extrapolation_model,
            IndCQC.interpolation_model,
        )
        .cast(pl.Float32)
        .alias(SHADOW_UNCLAMPED_RATIO)
    ).drop(
        IndCQC.extrapolation_forwards,
        IndCQC.extrapolation_model,
        IndCQC.interpolation_model,
    )


def build_bias_comparison(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Pre-aggregate the real and shadow ratios to one row per primary service type, size
    group, job role and month, so the diagnostic output stays small regardless of how
    many workplaces contribute.

    Args:
        estimated_job_role_posts_lf (pl.LazyFrame): dataset containing
            `imputed_ascwds_job_role_ratios`, `shadow_unclamped_ratio` and
            `ascwds_job_role_rolling_ratio`.

    Returns:
        pl.LazyFrame: one row per group and month, with the real and shadow means, the
            trendline value for reference, and the contributing workplace count to use
            as a weight when rolling this up further.
    """
    # polars_streaming: group_by falls back to the in-memory engine, but this is the
    # final reduction of a throwaway diagnostic run, not the production pipeline.
    return estimated_job_role_posts_lf.group_by(BIAS_COMPARISON_GROUPS).agg(
        pl.col(IndCQC.imputed_ascwds_job_role_ratios).mean().alias("mean_real_ratio"),
        pl.col(SHADOW_UNCLAMPED_RATIO).mean().alias("mean_shadow_ratio"),
        pl.col(IndCQC.ascwds_job_role_rolling_ratio).first().alias("trendline_ratio"),
        pl.len().alias("workplace_count"),
    )


def main(
    cleaned_data_source: str,
    bias_diagnostics_destination: str,
) -> None:
    """
    Instrumented copy of the job role imputation step, for one bias investigation.

    Args:
        cleaned_data_source (str): path to the cleaned data
        bias_diagnostics_destination (str): destination for the diagnostic comparison
            output
    """
    estimated_job_role_posts_lf = utils.scan_parquet(cleaned_data_source)

    estimated_job_role_posts_lf = iUtils.get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=IndCQC.ascwds_job_role_counts,
        output_col=IndCQC.ascwds_job_role_ratios,
    )

    estimated_job_role_posts_lf = iUtils.create_ascwds_job_role_rolling_ratio(
        estimated_job_role_posts_lf,
        extrapolation_period=NumericalValues.extrapolation_period,
        interpolation_cap_period=NumericalValues.interpolation_cap_period,
    )

    # Real pipeline output, byte-for-byte: clamped at zero, then re-shared across job
    # roles. Calling the production function unchanged so this is a comparison against
    # the actual pipeline, not an approximation of it.
    estimated_job_role_posts_lf = iUtils.add_imputed_ascwds_job_role_ratios(
        estimated_job_role_posts_lf
    )

    estimated_job_role_posts_lf = add_shadow_unclamped_ratio(
        estimated_job_role_posts_lf
    )

    comparison_lf = build_bias_comparison(estimated_job_role_posts_lf)

    utils.sink_to_parquet(
        lazy_df=comparison_lf,
        output_path=bias_diagnostics_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--bias_diagnostics_destination",
            "Destination s3 directory for the bias comparison output",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        bias_diagnostics_destination=args.bias_diagnostics_destination,
    )
