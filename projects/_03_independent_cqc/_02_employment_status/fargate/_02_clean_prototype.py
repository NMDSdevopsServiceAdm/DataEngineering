"""THROWAWAY diagnostics prototype - not part of the real pipeline.

Wraps this branch's raw-counts EmpStat ratio-filter clean job (still
.over()-based, like 2094-empstat, but sourced from raw counts instead of
_dedup, with dedup/percentage-share folded back into a single
create_employment_status_percentage_columns call, matching main's shape)
with RunDiagnostics to measure peak memory, for comparison against
2094-empstat's dedup-based equivalent prototype. Delete this file (and its
terraform/Dockerfile/Step Function wiring) once that comparison concludes.
"""

from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from projects._03_independent_cqc._02_employment_status.fargate.utils import (
    clean_utils as cUtils,
)
from utils.file_utils import split_s3_uri


def main(merged_data_source: str, cleaned_data_destination: str) -> None:
    """
    Runs this branch's raw-counts EmpStat clean job under RunDiagnostics.

    Args:
        merged_data_source (str): path to the merged data
        cleaned_data_destination (str): distinctly-named prototype output
            path - never the real pipeline's destination
    """
    data_bucket, _ = split_s3_uri(merged_data_source)
    diagnostics = RunDiagnostics("empstat_02_clean_raw", data_bucket).start()
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        lf = utils.scan_parquet(merged_data_source)
        diagnostics.checkpoint("start", lf)

        lf = cUtils.null_counts_for_low_org_ratio(lf)
        diagnostics.checkpoint("after_org_ratio", lf)

        lf = cUtils.null_counts_for_low_location_ratio(lf)
        diagnostics.checkpoint("after_location_ratio", lf)

        lf = cUtils.create_employment_status_percentage_columns(lf)
        diagnostics.checkpoint("after_dedup_and_percentage", lf)

        utils.sink_to_parquet(lazy_df=lf, output_path=cleaned_data_destination)
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--merged_data_source",
            "Source s3 directory for merged data",
        ),
        (
            "--cleaned_data_destination",
            "Distinctly-named prototype output path",
        ),
    )
    main(
        merged_data_source=args.merged_data_source,
        cleaned_data_destination=args.cleaned_data_destination,
    )
