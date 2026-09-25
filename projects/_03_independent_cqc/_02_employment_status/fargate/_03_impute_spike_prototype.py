import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_filter_utils as fUtils
import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_impute_utils as iUtils
from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from utils.file_utils import split_s3_uri

# Same streaming chunk size ticket 2000's prototypes ran with, so results compare.
pl.Config.set_streaming_chunk_size(50000)

EXTRAPOLATION_PERIOD = "2y"
INTERPOLATION_CAP_PERIOD = "5y"


def main(
    cleaned_data_source: str,
    spike_data_destination: str,
    diagnostics_job_name: str,
) -> None:
    """
    Ticket 2110 spike: drops zero job role filled posts rows, then runs L2 imputation.

    Candidate L2 (long job role, wide status) of the employment status shape
    comparison. Writes to a throwaway spike dataset, never the real impute output,
    and is instrumented with RunDiagnostics so peak memory can be compared against
    L1. Spike-only, not for merge.

    Args:
        cleaned_data_source (str): S3 path to the employment status clean output.
        spike_data_destination (str): S3 path for the throwaway spike output.
        diagnostics_job_name (str): RunDiagnostics job name, e.g.
            "empstat_spike_l2_red".
    """
    data_bucket, _ = split_s3_uri(cleaned_data_source)
    diagnostics = RunDiagnostics(diagnostics_job_name, data_bucket).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        lf = utils.scan_parquet(
            cleaned_data_source, selected_columns=fUtils.SPIKE_INPUT_COLUMNS
        )

        lf = fUtils.filter_out_zero_job_role_filled_posts(lf)
        diagnostics.checkpoint("after_filter", lf)

        lf = iUtils.add_rolling_employment_status_ratios(
            lf,
            extrapolation_period=EXTRAPOLATION_PERIOD,
            interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
        )
        diagnostics.checkpoint("after_rolling_ratio", lf)

        lf = iUtils.add_imputed_employment_status_rates(lf)
        diagnostics.checkpoint("after_impute", lf)

        utils.sink_to_parquet(lazy_df=lf, output_path=spike_data_destination)
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_data_source",
            "Source s3 directory for employment status cleaned data",
        ),
        (
            "--spike_data_destination",
            "Destination s3 directory for the throwaway spike output",
        ),
        (
            "--diagnostics_job_name",
            "RunDiagnostics job name, e.g. empstat_spike_l2_red",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        spike_data_destination=args.spike_data_destination,
        diagnostics_job_name=args.diagnostics_job_name,
    )
