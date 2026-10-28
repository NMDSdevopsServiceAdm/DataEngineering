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
# Distinct from a crash (1) so Publication-Output retries only this (EX_TEMPFAIL).
# Keep in step with the 75 in Publication-Output.json.
NOT_PROMOTED_EXIT_CODE = 75
MAX_PERIODS_REPORTED = 5
RELATIVE_DIFFERENCE = "relative_difference"


class ReconciliationMismatchError(ValueError):
    """The public output and archived estimate totals disagree beyond tolerance."""


def is_run_promoted(promoted_run_log_source: str, run_number: int) -> bool:
    """
    Checks whether a run's promotion to the long-term archive has finished.

    Promotion copies the run log last, so its partition marks a complete copy
    (the estimates partition appears as soon as its first file is copied). Lists
    partitions without reading data, so it is cheap to poll.

    Args:
        promoted_run_log_source (str): S3 directory of the long-term archive's
            run log, partitioned by archive_date and run_number
        run_number (int): the run to check

    Returns:
        bool: True if the run's run log partition is in the long-term archive
    """
    return run_number in file_utils.list_archive_runs(promoted_run_log_source)


def check_main(promoted_run_log_source: str, run_number: int) -> int:
    """
    Reports whether a run's promotion has finished, as the task's exit code.

    Args:
        promoted_run_log_source (str): S3 directory of the long-term archive's
            run log, partitioned by archive_date and run_number
        run_number (int): the run to check

    Returns:
        int: 0 if promoted, else NOT_PROMOTED_EXIT_CODE
    """
    promoted = is_run_promoted(promoted_run_log_source, run_number)
    state = "is" if promoted else "is not"
    print(f"Run {run_number} {state} in {promoted_run_log_source}")
    return 0 if promoted else NOT_PROMOTED_EXIT_CODE


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
    Compares a run's public and promoted archive filled posts for each period,
    marking the run reconciled in the run log if all match.

    Periods are those in the assessment data; other archive periods are
    ignored. The public figure is the assessment's pre-rounding national "All
    job roles" row (the public output only holds banded figures, and the
    assessment's other rows are overlapping rollups). The archive figure sums
    every job-role row, as the archive has no rollups.

    Args:
        assessment_source (str): S3 directory holding the assessed data of every
            run
        promoted_archive_source (str): S3 directory of the long-term promoted
            job-role estimates archive
        run_log_destination (str): S3 directory of the archive run log
        run_number (int): the run to reconcile

    Raises:
        ReconciliationMismatchError: if any period differs by more than
            RECONCILIATION_RELATIVE_TOLERANCE, a public period is missing from
            the archive, or the assessment has no national rows
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
    if public_by_period.is_empty():
        raise ReconciliationMismatchError(
            f"Run {run_number}: no national 'All job roles' rows in the assessment data"
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

    archive_column = IndCQC.estimate_filled_posts_by_job_role
    comparison_df = public_by_period.join(
        archive_by_period, on=IndCQC.cqc_location_import_date
    ).with_columns(
        (
            (pl.col(Pub.filled_posts) - pl.col(archive_column)).abs()
            / pl.col(archive_column)
        ).alias(RELATIVE_DIFFERENCE)
    )
    # Compared as "not within" so a NaN difference fails.
    differing_df = comparison_df.filter(
        ~(pl.col(RELATIVE_DIFFERENCE) <= RECONCILIATION_RELATIVE_TOLERANCE)
    ).sort(IndCQC.cqc_location_import_date)
    # max() skips NaN, so the count is what shows a NaN period.
    print(
        f"Run {run_number}: public output total {comparison_df[Pub.filled_posts].sum()}, "
        f"promoted archive total {comparison_df[archive_column].sum()}, largest "
        f"period difference {comparison_df[RELATIVE_DIFFERENCE].max():.4%}, "
        f"{differing_df.height} of {comparison_df.height} periods over tolerance"
    )
    if not differing_df.is_empty():
        details = "; ".join(
            f"{row[IndCQC.cqc_location_import_date]}: public {row[Pub.filled_posts]}, "
            f"archive {row[archive_column]}"
            for row in differing_df.head(MAX_PERIODS_REPORTED).iter_rows(named=True)
        )
        raise ReconciliationMismatchError(
            f"Run {run_number}: {differing_df.height} of {comparison_df.height} "
            f"periods differ by more than {RECONCILIATION_RELATIVE_TOLERANCE:.2%} "
            f"between the public output and promoted archive ({details})"
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
            "--promoted_run_log_source",
            "S3 directory of the long-term archive's run log (check mode)",
            False,
            None,
        ),
        (
            "--promoted_archive_source",
            "S3 directory of the long-term promoted job-role estimates archive (compare mode)",
            False,
            None,
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
        sys.exit(check_main(args.promoted_run_log_source, run_number))
    elif args.mode == "compare":
        main(
            assessment_source=args.assessment_source,
            promoted_archive_source=args.promoted_archive_source,
            run_log_destination=args.run_log_destination,
            run_number=run_number,
        )
    else:
        raise ValueError(f"Unknown mode '{args.mode}'. Use 'check' or 'compare'.")
