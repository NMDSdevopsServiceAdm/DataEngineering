import sys

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    run_log_utils,
)
from utils import file_utils
from utils.column_names.ind_cqc_pipeline_columns import ArchiveRunLogColumns as RunLog
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)

# The totals agree to ~1e-7 (Float32 rounding), so this leaves ~1000x headroom.
RECONCILIATION_RELATIVE_TOLERANCE = 0.0001


class ReconciliationMismatchError(ValueError):
    """The public output and archived estimate totals disagree beyond tolerance."""


def is_run_promoted(promoted_archive_source: str, run_number: int) -> bool:
    """
    Checks whether a run's job-role estimates are in the long-term archive yet.

    Lists partitions without reading data, so it is cheap to poll.

    Args:
        promoted_archive_source (str): S3 directory of the long-term promoted
            job-role estimates archive, partitioned by archive_date and
            run_number
        run_number (int): the run to check

    Returns:
        bool: True if the run's partition is present in the long-term archive
    """
    return run_number in file_utils.list_archive_runs(promoted_archive_source)


def _total_by_period(lf: pl.LazyFrame, value_column: str) -> pl.DataFrame:
    """
    Sums one column for each import date, as Float64 so the comparison isn't
    skewed by Float32 accumulation error.
    """
    return (
        lf.group_by(IndCQC.cqc_location_import_date)
        .agg(pl.col(value_column).cast(pl.Float64).sum().alias(value_column))
        .collect()
    )


def main(
    assessment_source: str,
    promoted_archive_source: str,
    run_log_destination: str,
    run_number: int,
) -> None:
    """
    Compares a run's public and promoted archive filled posts totals, marking
    the run reconciled in the run log on a match.

    Totals cover the periods in the assessment data; other archive periods are
    ignored. The public total is the assessment's pre-rounding national "All
    job roles" row (the public output only holds banded figures, and the
    assessment's other rows are overlapping rollups). The archive total sums
    every job-role row, as the archive has no rollups.

    Args:
        assessment_source (str): S3 directory holding the assessed data of every
            run
        promoted_archive_source (str): S3 directory of the long-term promoted
            job-role estimates archive
        run_log_destination (str): S3 directory of the archive run log
        run_number (int): the run to reconcile

    Raises:
        ReconciliationMismatchError: if the two totals differ by more than
            RECONCILIATION_RELATIVE_TOLERANCE
    """
    assessment_run_source, archive_run_source = merge_utils.resolve_run_sources(
        [assessment_source, promoted_archive_source], run_number
    )

    public_by_period = _total_by_period(
        utils.scan_parquet(assessment_run_source).filter(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == PublishedJobGroupLabels.all_job_roles,
            pl.col(IndCQC.current_region) == PublishedRegion.england,
            pl.col(IndCQC.primary_service_type) == PublishedMainService.all_locations,
        ),
        Pub.filled_posts,
    )
    public_periods = public_by_period[IndCQC.cqc_location_import_date]
    archive_by_period = _total_by_period(
        utils.scan_parquet(archive_run_source).filter(
            pl.col(IndCQC.cqc_location_import_date).is_in(public_periods.implode())
        ),
        IndCQC.estimate_filled_posts_by_job_role,
    )

    missing_periods = set(public_periods) - set(
        archive_by_period[IndCQC.cqc_location_import_date]
    )
    if missing_periods:
        raise ReconciliationMismatchError(
            f"Run {run_number}: periods in the public output but not in the "
            f"promoted archive: {sorted(missing_periods)}"
        )

    public_total = public_by_period[Pub.filled_posts].sum()
    archive_total = archive_by_period[IndCQC.estimate_filled_posts_by_job_role].sum()
    relative_difference = abs(public_total - archive_total) / archive_total
    print(
        f"Run {run_number}: public output total {public_total}, promoted archive "
        f"total {archive_total}, relative difference {relative_difference:.4%}"
    )
    if relative_difference > RECONCILIATION_RELATIVE_TOLERANCE:
        raise ReconciliationMismatchError(
            f"Run {run_number}: public output total {public_total} and promoted "
            f"archive total {archive_total} differ by {relative_difference:.2%}, "
            f"more than the {RECONCILIATION_RELATIVE_TOLERANCE:.2%} tolerance."
        )

    run_log_utils.mark_run_log_flag(run_log_destination, run_number, RunLog.reconciled)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--mode",
            "'check' (is the run promoted yet?) or 'compare' (reconcile totals)",
        ),
        ("--run_number", "The run to check or reconcile"),
        (
            "--promoted_archive_source",
            "S3 directory of the long-term promoted job-role estimates archive",
        ),
        (
            "--assessment_source",
            "S3 directory holding the assessed data of every run",
            False,
            None,
        ),
        (
            "--run_log_destination",
            "S3 directory of the archive run log",
            False,
            None,
        ),
    )
    run_number = merge_utils.parse_run_number(args.run_number, required=True)

    if args.mode == "check":
        promoted = is_run_promoted(args.promoted_archive_source, run_number)
        state = "is" if promoted else "is not"
        print(f"Run {run_number} {state} in {args.promoted_archive_source}")
        sys.exit(0 if promoted else 1)
    elif args.mode == "compare":
        main(
            assessment_source=args.assessment_source,
            promoted_archive_source=args.promoted_archive_source,
            run_log_destination=args.run_log_destination,
            run_number=run_number,
        )
    else:
        raise ValueError(f"Unknown mode '{args.mode}'. Use 'check' or 'compare'.")
