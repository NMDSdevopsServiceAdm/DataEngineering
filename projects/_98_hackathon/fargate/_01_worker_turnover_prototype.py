"""THROWAWAY memory diagnostic for _01_worker_turnover.py. Delete before merge.

Runs the real job's steps under RunDiagnostics and writes to a separate dataset,
so it cannot clobber the real output. DIAG_STOP_AFTER (set per execution by the
Diagnose-Hackathon-Worker-Turnover Step Function) picks where to sink and stop:

- filter: after the filters, `location_id` drop and main service type.
- flag: after the leaver flags.
- full: the whole job, including the raw worker join.

DIAG_MATERIALIZE picks how the filtered frame is shared by the later steps:

- none: leave it lazy, as the real job does.
- collect: `.collect().lazy()`, one in-memory copy.
- cache: `.cache()`, a lazy cache node.
- sink: write to local scratch, then scan that file.
"""

import os

import polars as pl

import projects._98_hackathon.fargate.utils.worker_turnover_utils as wUtils
from polars_utils import utils
from polars_utils.run_diagnostics import RunDiagnostics
from utils import file_utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def main(
    cleaned_ascwds_worker_source: str,
    raw_ascwds_worker_source: str,
    destination: str,
) -> None:
    """Runs the worker turnover job's steps under run diagnostics.

    Args:
        cleaned_ascwds_worker_source (str): path to the cleaned ASC-WDS worker data
        raw_ascwds_worker_source (str): path to the raw ASC-WDS worker data
        destination (str): throwaway destination, never the real output
    """
    stop_after = os.environ.get("DIAG_STOP_AFTER", "full")
    materialize = os.environ.get("DIAG_MATERIALIZE", "none")

    data_bucket, _ = file_utils.split_s3_uri(destination)
    diagnostics = RunDiagnostics(
        f"hackathon_proto_stop-{stop_after}_mat-{materialize}",
        data_bucket,
        sample_interval_seconds=1,
    ).start()
    print(f"Variant: stop_after={stop_after} materialize={materialize}")
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        worker_lf = utils.scan_parquet(cleaned_ascwds_worker_source)

        worker_lf = wUtils.filter_worker_turnover_data(worker_lf).drop(
            AWKClean.location_id
        )
        worker_lf = wUtils.add_main_service_type(worker_lf)

        if materialize == "collect":
            worker_lf = worker_lf.collect().lazy()
        elif materialize == "cache":
            worker_lf = worker_lf.cache()
        elif materialize == "sink":
            scratch_path = os.path.join(
                os.environ.get("POLARS_TEMP_DIR", "/tmp"), "filtered_worker.parquet"
            )
            worker_lf.sink_parquet(scratch_path)
            worker_lf = pl.scan_parquet(scratch_path)
        diagnostics.checkpoint("after_materialize")

        if stop_after != "filter":
            worker_lf = wUtils.add_is_leaver_flag(worker_lf)

        if stop_after == "full":
            raw_worker_lf = utils.scan_parquet(
                raw_ascwds_worker_source, schema=wUtils.RAW_WORKER_SCHEMA
            )
            worker_lf = wUtils.add_raw_worker_columns(worker_lf, raw_worker_lf)

        # Nothing executes until the sink, so these bracket the real run.
        diagnostics.checkpoint("before_sink", worker_lf)
        utils.sink_to_parquet(worker_lf, output_path=destination)
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--cleaned_ascwds_worker_source",
            "Source s3 directory for cleaned ascwds worker data",
        ),
        (
            "--raw_ascwds_worker_source",
            "Source s3 directory for raw ascwds worker data",
        ),
        ("--destination", "Throwaway destination s3 directory"),
    )
    main(
        cleaned_ascwds_worker_source=args.cleaned_ascwds_worker_source,
        raw_ascwds_worker_source=args.raw_ascwds_worker_source,
        destination=args.destination,
    )
