import polars as pl

import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_filter_utils as fUtils
import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_impute_utils as iUtils
import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_stress_utils as sUtils
from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.file_utils import split_s3_uri

# Same streaming chunk size ticket 2000's prototypes ran with, so results compare.
pl.Config.set_streaming_chunk_size(50000)

EXTRAPOLATION_PERIOD = "2y"
INTERPOLATION_CAP_PERIOD = "5y"

# 5 real statuses + 15 dummies = 20 categories, matching ticket 2000's stress point.
DUMMY_STATUS_COUNT = 15
NULL_PATTERN_SOURCE_COLUMN = EmpStatus.permanent_percentage


def main(
    cleaned_data_source: str,
    spike_data_destination: str,
    diagnostics_job_name: str,
) -> None:
    """
    Ticket 2110 N=20 stress test: runs L1 imputation with 15 dummy statuses added.

    Same as `_03_impute_spike_prototype.py` (zero job role filled posts rows
    dropped first), but with 15 synthetic statuses added after the filter, so
    candidate L1 (fully long) runs over 20 statuses. Writes to a throwaway stress
    dataset and is instrumented with RunDiagnostics. Spike-only, not for merge.

    Args:
        cleaned_data_source (str): S3 path to the employment status clean output.
        spike_data_destination (str): S3 path for the throwaway stress output.
        diagnostics_job_name (str): RunDiagnostics job name, e.g.
            "empstat_spike_l1_red_n20".
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

        lf, dummy_columns = sUtils.add_dummy_employment_status_columns(
            lf,
            dummy_count=DUMMY_STATUS_COUNT,
            null_pattern_source_column=NULL_PATTERN_SOURCE_COLUMN,
        )
        diagnostics.checkpoint("after_add_dummies", lf)
        percentage_columns = iUtils.EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS | dummy_columns

        long_lf = iUtils.reshape_employment_status_percentages_to_long_rows(
            lf, percentage_columns=percentage_columns
        )
        diagnostics.checkpoint("after_reshape", long_lf)

        long_lf = iUtils.add_rolling_employment_status_ratio(
            long_lf,
            extrapolation_period=EXTRAPOLATION_PERIOD,
            interpolation_cap_period=INTERPOLATION_CAP_PERIOD,
        )
        diagnostics.checkpoint("after_rolling_ratio", long_lf)

        long_lf = iUtils.add_imputed_employment_status_rates(long_lf)
        diagnostics.checkpoint("after_impute", long_lf)

        utils.sink_to_parquet(lazy_df=long_lf, output_path=spike_data_destination)
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
            "Destination s3 directory for the throwaway stress output",
        ),
        (
            "--diagnostics_job_name",
            "RunDiagnostics job name, e.g. empstat_spike_l1_red_n20",
        ),
    )
    main(
        cleaned_data_source=args.cleaned_data_source,
        spike_data_destination=args.spike_data_destination,
        diagnostics_job_name=args.diagnostics_job_name,
    )
