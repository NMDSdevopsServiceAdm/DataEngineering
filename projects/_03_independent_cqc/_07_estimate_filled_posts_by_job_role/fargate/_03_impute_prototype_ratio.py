"""THROWAWAY. Instrumented copy of _03_impute for one ratio-vs-nominal investigation.

Runs the real pipeline's nominal (additive) extrapolation alongside a ratio
(multiplicative) alternative, both put through the same floor-at-zero clip and
cross-job-role re-share, so the two methods are compared on equal footing.

The ratio method can't extrapolate against the rolling ratio trendline directly:
`model_extrapolation`'s ratio method divides by the trendline's value at a workplace's
last known date, and the trendline is exactly zero for a job role before it exists as
an option (e.g. support_worker before 2022). `add_rolling_ratio_index` builds a
chain-linked index of the trendline's own period-over-period change instead - starting
at 1 (no change), multiplying in each period's change, and treating a period whose
previous value is zero or missing as no change rather than an undefined division - so
it stays well-defined through a zero-then-rising trendline. It can still divide by zero
if the trendline itself falls to exactly zero from a nonzero value and a workplace's
last known date lands on or after that point; those rows are made null rather than left
to poison the group means they feed into.

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

# Duplicated from _03_impute.py's NumericalValues rather than imported: the deployed
# image copies pipeline entrypoint scripts flat into /app, so this file can't reach
# _03_impute.py by its source package path at runtime. Keep in sync with that file.
EXTRAPOLATION_PERIOD = "2y"
INTERPOLATION_CAP_PERIOD = "5y"

ROLLING_GROUPS = [
    IndCQC.primary_service_type,
    IndCQC.estimate_filled_posts_size_group,
    IndCQC.main_job_role_clean_labelled,
]
ORDER_KEY = IndCQC.cqc_location_import_date

ROLLING_RATIO_INDEX = "rolling_ratio_index"
RATIO_METHOD_UNNORMALISED = "ratio_method_unnormalised_ratio"
RATIO_METHOD_RATIO = "ratio_method_ratio"


def add_rolling_ratio_index(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Build a chain-linked index of the rolling ratio trendline's own period-over-period
    change, for use as the reference series for ratio (multiplicative) extrapolation.

    Built at group level (one row per primary service type, size group, job role and
    month, since every workplace within a group shares the same trendline value) and
    joined back onto the full location-level frame, rather than repeating identical work
    per location.

    Args:
        estimated_job_role_posts_lf (pl.LazyFrame): dataset already containing
            `ascwds_job_role_rolling_ratio`.

    Returns:
        pl.LazyFrame: dataset with the added `rolling_ratio_index` column.
    """
    trend_lf = (
        estimated_job_role_posts_lf.select(
            *ROLLING_GROUPS, ORDER_KEY, IndCQC.ascwds_job_role_rolling_ratio
        )
        .unique(subset=ROLLING_GROUPS + [ORDER_KEY])
        .sort(*ROLLING_GROUPS, ORDER_KEY)
    )

    previous_ratio = (
        pl.col(IndCQC.ascwds_job_role_rolling_ratio)
        .shift(1)
        .over(ROLLING_GROUPS, order_by=ORDER_KEY)
    )

    period_change = (
        pl.when(previous_ratio.is_null() | (previous_ratio == 0))
        .then(1.0)
        .otherwise(pl.col(IndCQC.ascwds_job_role_rolling_ratio) / previous_ratio)
    )

    trend_lf = trend_lf.with_columns(
        period_change.cum_prod()
        .over(ROLLING_GROUPS, order_by=ORDER_KEY)
        .alias(ROLLING_RATIO_INDEX)
    ).select(*ROLLING_GROUPS, ORDER_KEY, ROLLING_RATIO_INDEX)

    return estimated_job_role_posts_lf.join(
        trend_lf, on=ROLLING_GROUPS + [ORDER_KEY], how="left"
    )


def add_ratio_method_imputed_ratio(
    estimated_job_role_posts_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Compute the job role ratio using ratio (multiplicative) extrapolation against the
    rolling ratio index, put through the same floor-at-zero clip and cross-job-role
    re-share as the real nominal (additive) pipeline.

    Args:
        estimated_job_role_posts_lf (pl.LazyFrame): dataset containing
            `ascwds_job_role_ratios` and `rolling_ratio_index`.

    Returns:
        pl.LazyFrame: dataset with the added `ratio_method_ratio` column.
    """
    estimated_job_role_posts_lf = model_extrapolation(
        estimated_job_role_posts_lf,
        column_with_null_values=IndCQC.ascwds_job_role_ratios,
        model_to_extrapolate_from=ROLLING_RATIO_INDEX,
        extrapolation_method="ratio",
        group_columns=iUtils.JOB_ROLE_GROUPS,
    )
    estimated_job_role_posts_lf = model_interpolation(
        estimated_job_role_posts_lf,
        column_with_null_values=IndCQC.ascwds_job_role_ratios,
        method="trend",
        group_columns=iUtils.JOB_ROLE_GROUPS,
    )

    unnormalised = pl.coalesce(IndCQC.extrapolation_model, IndCQC.interpolation_model)
    # A workplace's own last-known-date index value can be zero (the trendline fell to
    # exactly zero by then), dividing to +/-inf or NaN under the ratio method - null
    # those out rather than let them poison the group mean this feeds into.
    unnormalised_safe = pl.when(unnormalised.is_finite()).then(unnormalised)

    estimated_job_role_posts_lf = estimated_job_role_posts_lf.with_columns(
        pl.when(pl.col(IndCQC.ascwds_job_role_ratios).is_null())
        .then(unnormalised_safe.clip(lower_bound=0))
        .cast(pl.Float32)
        .alias(RATIO_METHOD_UNNORMALISED)
    ).drop(
        IndCQC.extrapolation_forwards,
        IndCQC.extrapolation_model,
        IndCQC.interpolation_model,
    )

    estimated_job_role_posts_lf = iUtils.get_percent_share_ratios(
        estimated_job_role_posts_lf,
        input_col=RATIO_METHOD_UNNORMALISED,
        output_col=RATIO_METHOD_RATIO,
    )

    return estimated_job_role_posts_lf.with_columns(
        pl.coalesce(IndCQC.ascwds_job_role_ratios, RATIO_METHOD_RATIO)
        # A workplace whose only role(s) needing imputation last stood at zero
        # extrapolates to exactly zero under the ratio method (zero scaled by anything
        # is still zero), so the re-share divides zero by zero - resolve that as zero,
        # not undefined, rather than let it poison the group mean this feeds into.
        .fill_nan(0.0).alias(RATIO_METHOD_RATIO)
    ).drop(RATIO_METHOD_UNNORMALISED)


def build_ratio_comparison(estimated_job_role_posts_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Pre-aggregate the nominal and ratio-method ratios to one row per primary service
    type, size group, job role and month, so the diagnostic output stays small
    regardless of how many workplaces contribute.

    Args:
        estimated_job_role_posts_lf (pl.LazyFrame): dataset containing
            `imputed_ascwds_job_role_ratios`, `ratio_method_ratio` and
            `ascwds_job_role_rolling_ratio`.

    Returns:
        pl.LazyFrame: one row per group and month, with the nominal and ratio-method
            means, the trendline value for reference, and the contributing workplace
            count to use as a weight when rolling this up further.
    """
    # polars_streaming: group_by falls back to the in-memory engine, but this is the
    # final reduction of a throwaway diagnostic run, not the production pipeline.
    return estimated_job_role_posts_lf.group_by(ROLLING_GROUPS + [ORDER_KEY]).agg(
        pl.col(IndCQC.imputed_ascwds_job_role_ratios)
        .mean()
        .alias("mean_nominal_ratio"),
        pl.col(RATIO_METHOD_RATIO).mean().alias("mean_ratio_method_ratio"),
        pl.col(IndCQC.ascwds_job_role_rolling_ratio).first().alias("trendline_ratio"),
        pl.len().alias("workplace_count"),
    )


def main(
    cleaned_data_source: str,
    ratio_comparison_destination: str,
) -> None:
    """
    Instrumented copy of the job role imputation step, for one ratio-vs-nominal
    investigation.

    Args:
        cleaned_data_source (str): path to the cleaned data
        ratio_comparison_destination (str): destination for the diagnostic comparison
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
        extrapolation_period=EXTRAPOLATION_PERIOD,
        interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
    )

    # Real pipeline output, byte-for-byte, as the nominal-method reference point.
    estimated_job_role_posts_lf = iUtils.add_imputed_ascwds_job_role_ratios(
        estimated_job_role_posts_lf
    )

    estimated_job_role_posts_lf = add_rolling_ratio_index(estimated_job_role_posts_lf)
    estimated_job_role_posts_lf = add_ratio_method_imputed_ratio(
        estimated_job_role_posts_lf
    )

    comparison_lf = build_ratio_comparison(estimated_job_role_posts_lf)

    utils.sink_to_parquet(
        lazy_df=comparison_lf,
        output_path=ratio_comparison_destination,
    )


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--ratio_comparison_destination",
            "Destination s3 directory for the ratio-vs-nominal comparison output",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        ratio_comparison_destination=args.ratio_comparison_destination,
    )
