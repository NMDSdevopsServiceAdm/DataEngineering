import math
import sys
import tempfile
from pathlib import Path

import openpyxl
import polars as pl

from polars_utils import utils
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils import (
    merge_utils,
    output_workbook_utils,
    run_log_utils,
    workbook_helpers,
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


def _read_workbook_tables(workbook_path: Path) -> dict[str, list[tuple]]:
    """
    Reads each published table (header row, then data rows) from the workbook.

    Args:
        workbook_path (Path): the data download workbook

    Returns:
        dict[str, list[tuple]]: each table's rows, keyed by sheet name. A sheet or
            table the workbook lacks is missing from the result.
    """
    workbook = openpyxl.load_workbook(workbook_path, data_only=True)
    tables = {}
    for sheet_name, table_name in output_workbook_utils.TABLE_NAMES.items():
        if sheet_name not in workbook.sheetnames:
            continue
        sheet = workbook[sheet_name]
        if table_name not in sheet.tables:
            continue
        table_range = sheet.tables[table_name].ref
        tables[sheet_name] = [
            tuple(cell.value for cell in row) for row in sheet[table_range]
        ]
    return tables


def _values_match(workbook_value: object, prepared_value: object) -> bool:
    """Compares two cell values, allowing only float text noise on numbers."""
    # The workbook stores the prepared values exactly, so this is only text noise.
    float_tolerance = 1e-9
    both_numbers = all(
        isinstance(value, (int, float)) and not isinstance(value, bool)
        for value in (workbook_value, prepared_value)
    )
    if both_numbers:
        return math.isclose(
            workbook_value,
            prepared_value,
            rel_tol=float_tolerance,
            abs_tol=float_tolerance,
        )
    return workbook_value == prepared_value


def compare_download_tables(
    workbook_tables: dict[str, list[tuple]],
    prepared_tables: dict[str, list[tuple]],
    run_number: int,
) -> None:
    """
    Checks the workbook's published tables against the prepared tables: the same
    sheets, rows, columns and cell values.

    Args:
        workbook_tables (dict[str, list[tuple]]): rows read from the workbook
        prepared_tables (dict[str, list[tuple]]): the prepared rows, with the
            published labels and rounding, keyed by sheet name
        run_number (int): the run, for the error message

    Raises:
        ReconciliationMismatchError: if a sheet is missing or any content differs
    """
    for sheet in prepared_tables:
        if sheet not in workbook_tables:
            raise ReconciliationMismatchError(
                f"Run {run_number}: the data download workbook has no {sheet} table"
            )
    for sheet, prepared_rows in prepared_tables.items():
        if len(workbook_tables[sheet]) != len(prepared_rows):
            raise ReconciliationMismatchError(
                f"Run {run_number}: {sheet} has {len(workbook_tables[sheet]) - 1} data "
                f"rows in the workbook but {len(prepared_rows) - 1} in the prepared table"
            )
        if len(workbook_tables[sheet][0]) != len(prepared_rows[0]):
            raise ReconciliationMismatchError(
                f"Run {run_number}: {sheet} has {len(workbook_tables[sheet][0])} "
                f"columns in the workbook but {len(prepared_rows[0])} in the "
                "prepared table"
            )

    max_cells_reported = 5
    differences = []
    for sheet, prepared_rows in prepared_tables.items():
        for row_number, (workbook_row, prepared_row) in enumerate(
            zip(workbook_tables[sheet], prepared_rows)
        ):
            for column, (workbook_value, prepared_value) in enumerate(
                zip(workbook_row, prepared_row)
            ):
                if row_number == 0:
                    workbook_value = " ".join(str(workbook_value).split())
                    prepared_value = " ".join(str(prepared_value).split())
                if not _values_match(workbook_value, prepared_value):
                    differences.append(
                        f"{sheet} row {row_number} column {column + 1}: workbook "
                        f"{workbook_value!r}, prepared {prepared_value!r}"
                    )
    if differences:
        raise ReconciliationMismatchError(
            f"Run {run_number}: {len(differences)} cells differ between the workbook "
            f"and the prepared tables ({'; '.join(differences[:max_cells_reported])})"
        )


def reconcile_download(
    output_source: str, workbook_source: str, run_source: str
) -> None:
    """
    Reads a run's data download workbook back from S3 and checks it holds the
    prepared values: the tables rebuilt from the publication data, with the
    published labels and rounding (not the raw data). Fails with
    ReconciliationMismatchError if the workbook differs, and with the S3 error if
    it cannot be downloaded.

    Args:
        output_source (str): S3 directory of the publication data
        workbook_source (str): S3 directory of the data download workbooks
        run_source (str): the run's assessment directory, which names the workbook
            by its archive_date and run_number
    """
    run_number = merge_utils.run_number_from_source(run_source)
    workbook_name = workbook_helpers.generate_workbook_name(
        output_workbook_utils.DOWNLOAD_WORKBOOK_STEM, run_source
    )
    prepared_tables = {
        sheet: [tuple(table.columns)] + [tuple(row) for row in table.values.tolist()]
        for sheet, table in output_workbook_utils.prepare_download_tables(
            utils.scan_parquet(output_source)
        ).items()
    }
    with tempfile.TemporaryDirectory() as workbook_folder:
        workbook_path = Path(workbook_folder) / workbook_name
        workbook_helpers.download_from_s3(workbook_source, workbook_name, workbook_path)
        workbook_tables = _read_workbook_tables(workbook_path)
    compare_download_tables(workbook_tables, prepared_tables, run_number)


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

    Args:
        lf (pl.LazyFrame): rows holding the import date, job role and value
        value_column (str): the column to sum

    Returns:
        pl.DataFrame: one row per import date and job role
    """
    return (
        lf.group_by(
            IndCQC.cqc_location_import_date, IndCQC.main_job_role_clean_labelled
        )
        .agg(pl.col(value_column).cast(pl.Float64).sum().alias(value_column))
        .with_columns(pl.col(IndCQC.main_job_role_clean_labelled).cast(pl.String))
        .collect()
    )


def _relative_difference() -> pl.Expr:
    """
    The public and archive filled posts' absolute difference as a fraction of
    the archive's.

    Returns:
        pl.Expr: 0 where the two are equal, so 0 posts in both is not 0 / 0, and
            NaN where either is NaN
    """
    difference = (
        pl.col(Pub.unfiltered_filled_posts)
        - pl.col(IndCQC.estimate_filled_posts_by_job_role)
    ).abs()
    return (
        pl.when(difference == 0)
        .then(pl.lit(0.0))
        .otherwise(difference / pl.col(IndCQC.estimate_filled_posts_by_job_role).abs())
        .alias(RELATIVE_DIFFERENCE)
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

    comparison_df = comparison_df.with_columns(_relative_difference())
    # Compared as "not within" so a NaN difference fails. Worst first, so the
    # capped list names the largest differences.
    differing_df = comparison_df.filter(
        ~(pl.col(RELATIVE_DIFFERENCE) <= RECONCILIATION_RELATIVE_TOLERANCE)
    ).sort([RELATIVE_DIFFERENCE, *sort_columns], descending=[True, False, False])
    # max() skips NaN, so the count is what shows a NaN role.
    print(
        f"Run {run_number}: largest job role difference "
        f"{comparison_df[RELATIVE_DIFFERENCE].max():.4%}, {differing_df.height} of "
        f"{comparison_df.height} period and job role totals over tolerance"
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
    output_source: str,
    workbook_source: str,
    run_number: int,
) -> None:
    """
    Compares a run's public and promoted archive filled posts for each period,
    and for each period and job role, then checks its data download workbook
    holds the prepared values, marking the run reconciled in the run log only if
    all pass.

    Periods are those in the assessment data; other archive periods are
    ignored. The public figures are the assessment's pre-rounding national rows
    (the public output only holds banded figures, and the assessment's other
    rows are overlapping rollups): "All job roles" for each period's total, and
    one row per job role. The archive has no rollups, so its figures sum the
    job-role rows.

    Args:
        assessment_source (str): S3 directory holding the assessed data of every
            run
        promoted_archive_source (str): S3 directory of the long-term promoted
            job-role estimates archive
        run_log_destination (str): S3 directory of the archive run log
        output_source (str): S3 directory of the publication data the data
            download workbook was built from
        workbook_source (str): S3 directory of the data download workbooks
        run_number (int): the run to reconcile

    Raises:
        ReconciliationMismatchError: if any period, or period and job role,
            differs by more than RECONCILIATION_RELATIVE_TOLERANCE, a public
            period is missing from the archive, a job role is in only one
            source, the assessment has no national rows or a period's job role
            rows lack its "All job roles" row, or the data download workbook is
            missing or differs from the prepared tables
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
    periods_without_total = set(public_by_role[IndCQC.cqc_location_import_date]) - set(
        public_periods
    )
    if periods_without_total:
        raise ReconciliationMismatchError(
            f"Run {run_number}: periods with job role rows but no national 'All job "
            f"roles' row in the assessment data: "
            f"{', '.join(str(period) for period in sorted(periods_without_total))}"
        )
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
    ).with_columns(_relative_difference())
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

    reconcile_download(output_source, workbook_source, assessment_run_source)

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
        (
            "--output_source",
            "S3 directory of the publication data (compare mode)",
            False,
            None,
        ),
        (
            "--workbook_source",
            "S3 directory of the data download workbooks (compare mode)",
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
            output_source=args.output_source,
            workbook_source=args.workbook_source,
            run_number=run_number,
        )
    else:
        raise ValueError(f"Unknown mode '{args.mode}'. Use 'check' or 'compare'.")
