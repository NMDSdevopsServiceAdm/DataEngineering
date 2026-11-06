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
MAX_DIFFERENCES_REPORTED = 5
RELATIVE_DIFFERENCE = "relative_difference"


class ReconciliationMismatchError(ValueError):
    """The public output and archived estimate totals disagree beyond tolerance."""


def is_run_promoted(promoted_metadata_source: str, run_number: int) -> bool:
    """
    Checks whether a run's promotion to the long-term archive has finished.

    Metadata is copied last and is a single file, so its partition marks a
    complete copy (estimates appear at their first file). Lists partitions without
    reading data, so it is cheap to poll.

    Args:
        promoted_metadata_source (str): S3 directory of the long-term archive's
            metadata, partitioned by archive_date and run_number
        run_number (int): the run to check

    Returns:
        bool: True if the run's metadata partition is in the long-term archive
    """
    return run_number in file_utils.list_archive_runs(promoted_metadata_source)


def check_main(promoted_metadata_source: str, run_number: int) -> int:
    """
    Reports whether a run's promotion has finished, as the task's exit code.

    Args:
        promoted_metadata_source (str): S3 directory of the long-term archive's
            metadata, partitioned by archive_date and run_number
        run_number (int): the run to check

    Returns:
        int: 0 if promoted, else NOT_PROMOTED_EXIT_CODE
    """
    promoted = is_run_promoted(promoted_metadata_source, run_number)
    state = "is" if promoted else "is not"
    print(f"Run {run_number} {state} in {promoted_metadata_source}")
    return 0 if promoted else NOT_PROMOTED_EXIT_CODE


def _total_by_period_and_role(lf: pl.LazyFrame, value_column: str) -> pl.DataFrame:
    """
    Sums one column for each import date and job role, as Float64 so the
    comparison isn't skewed by Float32 accumulation error. The job role is
    cast to String so both sources join on the same type.
    """
    return (
        lf.group_by(
            IndCQC.cqc_location_import_date, IndCQC.main_job_role_clean_labelled
        )
        .agg(pl.col(value_column).cast(pl.Float64).sum().alias(value_column))
        .with_columns(pl.col(IndCQC.main_job_role_clean_labelled).cast(pl.String))
        .collect()
    )


def _raise_if_roles_differ(
    public_by_role: pl.DataFrame, archive_by_role: pl.DataFrame, run_number: int
) -> None:
    """
    Raises if any period and job role differs between the public output and
    promoted archive, naming up to MAX_DIFFERENCES_REPORTED of them.

    A period and job role in only one source is reported before any difference
    in value, since a missing role has nothing to compare.

    Args:
        public_by_role (pl.DataFrame): public filled posts per period and job role
        archive_by_role (pl.DataFrame): archive estimates per period and job role
        run_number (int): the run being reconciled, for the error message

    Raises:
        ReconciliationMismatchError: if a period and job role is in only one
            source, or differs by more than RECONCILIATION_RELATIVE_TOLERANCE
    """
    public_column = Pub.unfiltered_filled_posts
    archive_column = IndCQC.estimate_filled_posts_by_job_role
    sort_columns = [
        IndCQC.cqc_location_import_date,
        IndCQC.main_job_role_clean_labelled,
    ]

    comparison_df = public_by_role.join(
        archive_by_role, on=sort_columns, how="full", coalesce=True
    )

    one_sided_df = comparison_df.filter(
        pl.col(public_column).is_null() | pl.col(archive_column).is_null()
    ).sort(sort_columns)
    if not one_sided_df.is_empty():
        details = "; ".join(
            f"{row[IndCQC.cqc_location_import_date]} "
            f"{row[IndCQC.main_job_role_clean_labelled]} (only in the "
            f"{'archive' if row[public_column] is None else 'public output'})"
            for row in one_sided_df.head(MAX_DIFFERENCES_REPORTED).iter_rows(named=True)
        )
        raise ReconciliationMismatchError(
            f"Run {run_number}: {one_sided_df.height} period and job role "
            f"combinations are in only one of the public output and promoted "
            f"archive ({details})"
        )

    # Compared as "not within" so a NaN difference fails.
    differing_df = (
        comparison_df.with_columns(
            (
                (pl.col(public_column) - pl.col(archive_column)).abs()
                / pl.col(archive_column)
            ).alias(RELATIVE_DIFFERENCE)
        )
        .filter(~(pl.col(RELATIVE_DIFFERENCE) <= RECONCILIATION_RELATIVE_TOLERANCE))
        .sort(sort_columns)
    )
    if not differing_df.is_empty():
        details = "; ".join(
            f"{row[IndCQC.cqc_location_import_date]} "
            f"{row[IndCQC.main_job_role_clean_labelled]}: "
            f"public {row[public_column]}, archive {row[archive_column]}"
            for row in differing_df.head(MAX_DIFFERENCES_REPORTED).iter_rows(named=True)
        )
        raise ReconciliationMismatchError(
            f"Run {run_number}: {differing_df.height} of {comparison_df.height} "
            f"period and job role totals differ by more than "
            f"{RECONCILIATION_RELATIVE_TOLERANCE:.2%} between the public output "
            f"and promoted archive ({details})"
        )


def main(
    assessment_source: str,
    promoted_archive_source: str,
    run_log_destination: str,
    run_number: int,
) -> None:
    """
    Compares a run's public and promoted archive filled posts for each period
    and for each period and job role, marking the run reconciled in the run log
    if all match.

    Periods are those in the assessment data; other archive periods are
    ignored. The public figures are the assessment's pre-rounding national rows
    (the public output only holds banded figures, and the assessment's other
    rows are overlapping rollups): "All job roles" for the period total, and one
    row per job role. The archive figures sum every job-role row for the period,
    and each job role's rows for that role, as the archive has no rollups.

    Args:
        assessment_source (str): S3 directory holding the assessed data of every
            run
        promoted_archive_source (str): S3 directory of the long-term promoted
            job-role estimates archive
        run_log_destination (str): S3 directory of the archive run log
        run_number (int): the run to reconcile

    Raises:
        ReconciliationMismatchError: if any period, or period and job role,
            differs by more than RECONCILIATION_RELATIVE_TOLERANCE, a public
            period is missing from the archive, a job role is in only one
            source, or the assessment has no national rows
    """
    assessment_run_source, archive_run_source = merge_utils.resolve_run_sources(
        [assessment_source, promoted_archive_source], run_number
    )

    public_by_role = _total_by_period_and_role(
        utils.scan_parquet(assessment_run_source).filter(
            pl.col(IndCQC.current_region) == PublishedRegion.england,
            pl.col(IndCQC.primary_service_type) == PublishedMainService.all_locations,
        ),
        Pub.unfiltered_filled_posts,
    )
    is_all_job_roles = (
        pl.col(IndCQC.main_job_role_clean_labelled)
        == PublishedJobGroupLabels.all_job_roles
    )
    public_by_period = public_by_role.filter(is_all_job_roles).drop(
        IndCQC.main_job_role_clean_labelled
    )
    if public_by_period.is_empty():
        raise ReconciliationMismatchError(
            f"Run {run_number}: no national 'All job roles' rows in the assessment data"
        )
    public_by_role = public_by_role.filter(~is_all_job_roles)
    public_periods = public_by_period[IndCQC.cqc_location_import_date]
    archive_by_role = _total_by_period_and_role(
        utils.scan_parquet(archive_run_source).filter(
            pl.col(IndCQC.cqc_location_import_date).is_in(public_periods.implode())
        ),
        IndCQC.estimate_filled_posts_by_job_role,
    )
    archive_by_period = archive_by_role.group_by(IndCQC.cqc_location_import_date).agg(
        pl.col(IndCQC.estimate_filled_posts_by_job_role).sum()
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
            (pl.col(Pub.unfiltered_filled_posts) - pl.col(archive_column)).abs()
            / pl.col(archive_column)
        ).alias(RELATIVE_DIFFERENCE)
    )
    # Compared as "not within" so a NaN difference fails.
    differing_df = comparison_df.filter(
        ~(pl.col(RELATIVE_DIFFERENCE) <= RECONCILIATION_RELATIVE_TOLERANCE)
    ).sort(IndCQC.cqc_location_import_date)
    # max() skips NaN, so the count is what shows a NaN period.
    print(
        f"Run {run_number}: public output total {comparison_df[Pub.unfiltered_filled_posts].sum()}, "
        f"promoted archive total {comparison_df[archive_column].sum()}, largest "
        f"period difference {comparison_df[RELATIVE_DIFFERENCE].max():.4%}, "
        f"{differing_df.height} of {comparison_df.height} periods over tolerance"
    )
    if not differing_df.is_empty():
        details = "; ".join(
            f"{row[IndCQC.cqc_location_import_date]}: public {row[Pub.unfiltered_filled_posts]}, "
            f"archive {row[archive_column]}"
            for row in differing_df.head(MAX_DIFFERENCES_REPORTED).iter_rows(named=True)
        )
        raise ReconciliationMismatchError(
            f"Run {run_number}: {differing_df.height} of {comparison_df.height} "
            f"periods differ by more than {RECONCILIATION_RELATIVE_TOLERANCE:.2%} "
            f"between the public output and promoted archive ({details})"
        )

    _raise_if_roles_differ(public_by_role, archive_by_role, run_number)

    run_log_utils.mark_run_log_flag(run_log_destination, run_number, RunLog.reconciled)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--mode",
            "'check' (is the run promoted yet?) or 'compare' (reconcile totals)",
        ),
        ("--run_number", "The run to check or reconcile"),
        (
            "--promoted_metadata_source",
            "S3 directory of the long-term archive's metadata (check mode)",
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
        sys.exit(check_main(args.promoted_metadata_source, run_number))
    elif args.mode == "compare":
        main(
            assessment_source=args.assessment_source,
            promoted_archive_source=args.promoted_archive_source,
            run_log_destination=args.run_log_destination,
            run_number=run_number,
        )
    else:
        raise ValueError(f"Unknown mode '{args.mode}'. Use 'check' or 'compare'.")
