"""THROWAWAY. Instrumented copy of _03_impute.py for one memory investigation.

Mirrors _03_impute.main exactly, with a RunDiagnostics checkpoint either side of each
step, so the RSS curve can be attributed to a stage rather than to the job as a whole.
The open question is whether the trend impute's window functions push peak memory near
the task's 60GB ceiling, or whether the runtime is spent somewhere else entirely.

Writes to its own dataset name so it can never overwrite the real pipeline's output.
Delete this file, its Dockerfile COPY line, its terraform module and its step function
definition once the investigation concludes.
"""

import polars as pl

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.impute_utils as iUtils
from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from polars_utils.utils import split_s3_uri
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

# Set streaming chunk size for memory management - each thread (per CPU core) will load
# in a chunk of this size.
pl.Config.set_streaming_chunk_size(50000)

SAMPLE_INTERVAL_SECONDS: float = 5


def main(
    cleaned_data_source: str,
    imputed_data_destination: str,
) -> None:
    """
    Instrumented copy of the job role imputation step.

    Args:
        cleaned_data_source (str): path to the cleaned data
        imputed_data_destination (str): destination for output
    """
    data_bucket, _ = split_s3_uri(imputed_data_destination)
    diagnostics = RunDiagnostics(
        "_07_03_impute_prototype",
        data_bucket,
        sample_interval_seconds=SAMPLE_INTERVAL_SECONDS,
    ).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        estimated_job_role_posts_lf = utils.scan_parquet(cleaned_data_source)
        diagnostics.checkpoint("after_scan", estimated_job_role_posts_lf)

        estimated_job_role_posts_lf = iUtils.get_percent_share_ratios(
            estimated_job_role_posts_lf,
            input_col=IndCQC.ascwds_job_role_counts,
            output_col=IndCQC.ascwds_job_role_ratios,
        )
        diagnostics.checkpoint("after_ratios", estimated_job_role_posts_lf)

        estimated_job_role_posts_lf = iUtils.create_ascwds_job_role_rolling_ratio(
            estimated_job_role_posts_lf
        )
        diagnostics.checkpoint("after_rolling_ratio", estimated_job_role_posts_lf)

        estimated_job_role_posts_lf = iUtils.add_imputed_ascwds_job_role_ratios(
            estimated_job_role_posts_lf
        )
        diagnostics.checkpoint("after_trend_impute", estimated_job_role_posts_lf)

        estimated_job_role_posts_lf = iUtils.add_imputed_ascwds_job_role_counts(
            estimated_job_role_posts_lf
        )
        diagnostics.checkpoint("before_sink", estimated_job_role_posts_lf)

        utils.sink_to_parquet(
            lazy_df=estimated_job_role_posts_lf,
            output_path=imputed_data_destination,
        )
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--imputed_data_destination",
            "Destination s3 directory for imputed data",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        imputed_data_destination=args.imputed_data_destination,
    )
