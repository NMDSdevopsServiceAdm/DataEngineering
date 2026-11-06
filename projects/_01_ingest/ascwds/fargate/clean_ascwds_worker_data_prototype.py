"""THROWAWAY memory diagnostic for clean_ascwds_worker_data.py. Delete before merge.

Runs the real clean job's steps under RunDiagnostics and writes to a separate
dataset, so it cannot clobber the real output. Two environment variables pick
the variant (set per execution by the Diagnose-ASCWDS-Worker-Clean Step Function):

- DIAG_SKIP: comma-separated changes to leave out.
    - estab_cat: keep the establishment ID as a string, not categorical.
    - mainstid: do not read the main service type ID column.
- DIAG_STOP_AFTER: where to sink and stop.
    - join: after removing workers without workplaces.
    - impute: after the main job role is cleaned (the `.over()` step).
    - full: the whole job.
"""

import os

import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.raw_data_adjustments import is_unique_worker_data
from polars_utils.run_diagnostics import RunDiagnostics
from projects._01_ingest.ascwds.fargate import clean_ascwds_worker_data as realJob
from projects._01_ingest.ascwds.fargate.utils import clean_worker_utils as wUtils
from utils import file_utils
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_values.ascwds_labelled_vocab import EMPLOYMENT_STATUS, MAIN_JOB_ROLE


def main(
    worker_source: str,
    cleaned_workplace_source: str,
    destination: str,
) -> None:
    """Runs the clean worker job's steps under run diagnostics.

    Args:
        worker_source (str): path to the raw ASC-WDS worker data
        cleaned_workplace_source (str): path to the cleaned ASC-WDS workplace data
        destination (str): throwaway destination, never the real cleaned output
    """
    skip = {item for item in os.environ.get("DIAG_SKIP", "").split(",") if item}
    stop_after = os.environ.get("DIAG_STOP_AFTER", "full")
    variant = f"skip-{'-'.join(sorted(skip)) or 'none'}_stop-{stop_after}"

    data_bucket, _ = file_utils.split_s3_uri(destination)
    diagnostics = RunDiagnostics(
        f"clean_worker_proto_{variant}", data_bucket, sample_interval_seconds=1
    ).start()
    print(f"Variant: {variant}")
    print(f"Run diagnostics: s3://{diagnostics.bucket}/{diagnostics.prefix}")

    try:
        worker_schema = {
            column: dtype
            for column, dtype in realJob.WORKER_SCHEMA.items()
            if not ("mainstid" in skip and column == AWKClean.main_service_type_id)
        }
        worker_lf = utils.scan_parquet(worker_source, schema=worker_schema).select(
            worker_schema.keys()
        )
        worker_lf = cUtils.column_to_date(
            worker_lf, AWKClean.import_date, AWKClean.ascwds_worker_import_date
        )
        worker_lf = worker_lf.filter(is_unique_worker_data())

        workplace_lf = utils.scan_parquet(
            cleaned_workplace_source, schema=realJob.WORKPLACE_SCHEMA
        ).select(realJob.WORKPLACE_SCHEMA.keys())

        if "estab_cat" in skip:
            workplace_lf = workplace_lf.with_columns(
                pl.col(AWKClean.establishment_id).cast(pl.String)
            )
        else:
            worker_lf = worker_lf.with_columns(
                pl.col(AWKClean.establishment_id).cast(
                    CategoricalColumnTypes.EstablishmentCatType
                )
            )

        worker_lf = wUtils.remove_workers_without_workplaces(worker_lf, workplace_lf)
        worker_lf = worker_lf.drop(AWKClean.import_date)

        if stop_after != "join":
            data_labels_lf = cUtils.build_labels_lf(
                {
                    AWKClean.main_job_role_clean: MAIN_JOB_ROLE.code_to_label(),
                    AWKClean.employment_status_clean: EMPLOYMENT_STATUS.code_to_label(),
                }
            )
            worker_lf = wUtils.create_clean_main_job_role_column(
                worker_lf, data_labels_lf
            )

        if stop_after == "full":
            worker_lf = wUtils.create_clean_employment_status_column(
                worker_lf, data_labels_lf
            )
            worker_lf = worker_lf.with_columns(
                pl.col(AWKClean.location_id).cast(
                    CategoricalColumnTypes.LocationCatType
                ),
                pl.col(AWKClean.main_job_role_clean).cast(
                    CategoricalColumnTypes.MainJobRoleIdCatType
                ),
                pl.col(AWKClean.main_job_role_clean_labelled).cast(
                    CategoricalColumnTypes.JobRoleCatType
                ),
                pl.col(AWKClean.employment_status_clean).cast(
                    CategoricalColumnTypes.EmploymentStatusIdCatType
                ),
                pl.col(AWKClean.employment_status_clean_labelled).cast(
                    CategoricalColumnTypes.EmploymentStatusCatType
                ),
            )

        # Nothing executes until the sink, so these bracket the real run.
        diagnostics.checkpoint("before_sink", worker_lf)
        utils.sink_to_parquet(worker_lf, output_path=destination)
        diagnostics.checkpoint("after_sink")
    finally:
        diagnostics.stop()


if __name__ == "__main__":
    args = utils.get_args(
        ("--worker_source", "Source s3 directory for raw ASC-WDS worker data"),
        (
            "--cleaned_workplace_source",
            "Source s3 directory for cleaned ASC-WDS workplace data",
        ),
        ("--destination", "Throwaway destination s3 directory"),
    )
    main(
        worker_source=args.worker_source,
        cleaned_workplace_source=args.cleaned_workplace_source,
        destination=args.destination,
    )
