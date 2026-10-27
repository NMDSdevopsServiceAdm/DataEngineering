import sys
from datetime import date

import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    publication_utils,
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

RECONCILIATION_RELATIVE_TOLERANCE = 0.01


class ReconciliationMismatchError(ValueError):
    """The public output and archived estimate totals disagree beyond tolerance."""


def is_run_promoted(promoted_archive_source: str, run_number: int) -> bool:
    """
    Checks whether a run's job-role estimates have landed in the long-term
    archive yet.

    Cheap readiness check: lists the archive's run partitions rather than
    reading any data, so the pipeline can poll it while waiting for
    `update_run_log.sh`'s promotion step to complete.

    Args:
        promoted_archive_source (str): S3 directory of the long-term promoted
            job-role estimates archive, partitioned by archive_date and
            run_number
        run_number (int): the run to check

    Returns:
        bool: True if the run's partition is present in the long-term archive
    """
    return run_number in file_utils.list_archive_runs(promoted_archive_source)


def _period_total(lf: pl.LazyFrame, value_column: str, today: date) -> float:
    """
    Sums one column over the shared reduced period.

    Summed as Float64 so the comparison measures real differences, not Float32
    accumulation error over a long sum.
    """
    return (
        lf.filter(publication_utils.reduced_data_filter_expr(today=today))
        .select(pl.col(value_column).cast(pl.Float64).sum())
        .collect()
        .item()
    )


def main(
    assessment_source: str,
    promoted_archive_source: str,
    run_log_destination: str,
    run_number: int,
) -> None:
    """
    Reconciles one run's public output against its promoted archive, marking the
    run reconciled in the run log on a match.

    Both totals are recomputed independently over the same reduced period, so
    the comparison holds regardless of how each pipeline's own upstream sampling
    differs. The public total is the assessment store's raw, pre-rounding
    filled posts (never the public output's banded figure, which stays the only
    filled-posts column the public dataset carries), taken from the national
    "All job roles" row only, as the assessment data also holds overlapping
    regional, service and job role rollups. The archive total sums every
    job-role row of estimate_filled_posts_by_job_role in the promoted archive,
    which has no rollup rows.

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
    today = date.today()
    assessment_run_source, archive_run_source = merge_utils.resolve_run_sources(
        [assessment_source, promoted_archive_source], run_number
    )

    national_total_lf = utils.scan_parquet(assessment_run_source).filter(
        pl.col(IndCQC.main_job_role_clean_labelled)
        == PublishedJobGroupLabels.all_job_roles,
        pl.col(IndCQC.current_region) == PublishedRegion.england,
        pl.col(IndCQC.primary_service_type) == PublishedMainService.all_locations,
    )
    public_total = _period_total(national_total_lf, Pub.filled_posts, today)
    archive_total = _period_total(
        utils.scan_parquet(archive_run_source),
        IndCQC.estimate_filled_posts_by_job_role,
        today,
    )

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
