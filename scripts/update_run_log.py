"""
Set a flag to True for one run in a local copy of the archive run log, or list
the month's runs.

Works on local files only: `update_run_log.sh` copies the run log down from S3,
runs this script as a module from the repo root
(`python -m scripts.update_run_log`), and uploads the changed partitions.
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Optional, Sequence

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as Keys,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveRunLogColumns as RunLog,
)

SUPPORTED_FLAGS: tuple[str, ...] = (
    RunLog.approved,
    RunLog.selected_for_publication,
    RunLog.reconciled,
    RunLog.locally_checked,
)
RUN_TABLE_COLUMNS: tuple[str, ...] = (
    Keys.run_number,
    Keys.archive_date,
    *SUPPORTED_FLAGS,
    RunLog.tag,
)


def set_run_flag(
    run_log_df: pl.DataFrame, run_number: int, flag: str, month_start: date
) -> pl.DataFrame:
    """Sets a flag to True for one run.

    Selecting a run moves the month's selection: `selected_for_publication` is
    cleared on the other runs archived since `month_start`.

    Args:
        run_log_df (pl.DataFrame): The run log, including the partition key columns.
        run_number (int): The run to update.
        flag (str): The flag column to set. One of `SUPPORTED_FLAGS`.
        month_start (date): The first day of the month being worked on.

    Returns:
        pl.DataFrame: The run log with the flag set to True for the run.

    Raises:
        ValueError: If the flag is not supported, the run number matches no row or
            more than one row, or the flag is already True for the run. Selecting
            also raises if the run is not approved or was archived before
            `month_start`.
    """
    if flag not in SUPPORTED_FLAGS:
        raise ValueError(
            f"Flag '{flag}' is not supported. Use one of {SUPPORTED_FLAGS}."
        )

    is_run = pl.col(Keys.run_number) == run_number
    run_df = run_log_df.filter(is_run)

    if run_df.height == 0:
        raise ValueError(f"Run number {run_number} not found in the run log.")
    if run_df.height > 1:
        raise ValueError(
            f"Run number {run_number} matches multiple rows in the run log."
        )

    is_selection = flag == RunLog.selected_for_publication
    # Before the already-True check, so an earlier month's selection says why it can't be reselected.
    if is_selection:
        if not run_df[RunLog.approved].item():
            raise ValueError(f"Run number {run_number} is not approved.")
        if run_df[Keys.archive_date].item() < month_start.isoformat():
            raise ValueError(
                f"Run number {run_number} was archived before {month_start}."
            )

    if run_df[flag].item() is True:
        raise ValueError(f"Run number {run_number} is already {flag}.")

    if is_selection:
        run_log_df = run_log_df.with_columns(
            pl.when(pl.col(Keys.archive_date) >= month_start.isoformat())
            .then(False)
            .otherwise(pl.col(flag))
            .alias(flag)
        )

    return run_log_df.with_columns(
        pl.when(is_run).then(True).otherwise(pl.col(flag)).alias(flag)
    )


def get_changed_run_numbers(
    run_log_df: pl.DataFrame, updated_df: pl.DataFrame, flag: str
) -> list[int]:
    """Finds the runs whose flag differs between two versions of the run log.

    Args:
        run_log_df (pl.DataFrame): The run log before the update.
        updated_df (pl.DataFrame): The run log after the update, in the same row order.
        flag (str): The flag column that was updated.

    Returns:
        list[int]: The run numbers whose flag changed, counting a null as different.
    """
    return updated_df.filter(updated_df[flag].ne_missing(run_log_df[flag]))[
        Keys.run_number
    ].to_list()


def format_run_table(run_log_df: pl.DataFrame, month_start: date) -> str:
    """Formats the runs archived on or after `month_start` as an aligned text table.

    Args:
        run_log_df (pl.DataFrame): The run log, including the partition key columns.
        month_start (date): The first day of the month being worked on.

    Returns:
        str: The table, or a message when no run is in the window.
    """
    runs_df = (
        run_log_df.filter(pl.col(Keys.archive_date) >= month_start.isoformat())
        .sort(Keys.run_number)
        .select(RUN_TABLE_COLUMNS)
    )
    if runs_df.is_empty():
        return f"No runs archived on or after {month_start}."

    rows = [list(RUN_TABLE_COLUMNS)] + [
        ["" if value is None else str(value) for value in row] for row in runs_df.rows()
    ]
    widths = [max(len(row[i]) for row in rows) for i in range(len(RUN_TABLE_COLUMNS))]
    return "\n".join(
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in rows
    )


def read_run_log(run_log_dir: Path) -> pl.DataFrame:
    """Reads a local run log, taking the partition keys from the folder names.

    Eager read: the run log has one row per run, so it is small.

    Args:
        run_log_dir (Path): Local copy of the run log, laid out as
            `archive_date=<date>/run_number=<n>/<file>.parquet`.

    Returns:
        pl.DataFrame: The run log including the partition key columns.

    Raises:
        FileNotFoundError: If the directory has no parquet files.
    """
    if not any(run_log_dir.rglob("*.parquet")):
        raise FileNotFoundError(f"No parquet files found in {run_log_dir}.")

    return pl.read_parquet(
        run_log_dir / "**" / "*.parquet",
        hive_partitioning=True,
        hive_schema={Keys.archive_date: pl.String, Keys.run_number: pl.Int64},
    )


def write_run_partition(
    run_log_df: pl.DataFrame, run_number: int, output_dir: Path
) -> None:
    """Writes one run's row in the run log layout, without the partition keys.

    Args:
        run_log_df (pl.DataFrame): The run log, including the partition key columns.
        run_number (int): The run to write.
        output_dir (Path): Directory to write the run's partition folder into.
    """
    run_df = run_log_df.filter(pl.col(Keys.run_number) == run_number)
    archive_date = run_df[Keys.archive_date].item()

    partition_dir = (
        output_dir
        / f"{Keys.archive_date}={archive_date}"
        / f"{Keys.run_number}={run_number}"
    )
    partition_dir.mkdir(parents=True)
    run_df.drop(Keys.archive_date, Keys.run_number).write_parquet(
        partition_dir / "file.parquet"
    )


def _parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """Parses the command line arguments.

    Args:
        argv (Optional[Sequence[str]]): Command line arguments; defaults to `sys.argv`.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run_log_dir", type=Path, required=True, help="Local run log")
    parser.add_argument("--output_dir", type=Path, help="Updated partitions")
    parser.add_argument("--run_number", type=int)
    parser.add_argument("--flag")
    parser.add_argument(
        "--month_start",
        type=date.fromisoformat,
        default=date.today().replace(day=1),
        help="First day of the month being worked on (yyyy-mm-dd); default this month",
    )
    parser.add_argument(
        "--list", action="store_true", help="List the month's runs and exit"
    )
    args = parser.parse_args(argv)

    if not args.list and None in (args.output_dir, args.run_number, args.flag):
        parser.error(
            "--output_dir, --run_number and --flag are required without --list"
        )
    return args


def _format_row(row_df: pl.DataFrame) -> str:
    """Formats a one-row frame as plain `column: value` lines (ASCII-safe)."""
    row = row_df.row(0, named=True)
    return "\n".join(f"  {column}: {value}" for column, value in row.items())


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Lists the month's runs, or updates one run's flag.

    An update writes only the runs whose flag changed to the output dir, and
    nothing if the update is rejected. The input directory is never modified.

    Args:
        argv (Optional[Sequence[str]]): Command line arguments; defaults to `sys.argv`.

    Returns:
        int: 0 on success, 1 if the run log could not be read or the update was
            rejected.
    """
    args = _parse_arguments(argv)

    try:
        run_log_df = read_run_log(args.run_log_dir)
        if args.list:
            print(format_run_table(run_log_df, args.month_start))
            return 0
        updated_df = set_run_flag(
            run_log_df, args.run_number, args.flag, args.month_start
        )
    except (ValueError, FileNotFoundError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    for run_number in get_changed_run_numbers(run_log_df, updated_df, args.flag):
        is_run = pl.col(Keys.run_number) == run_number
        print(f"Run {run_number}:")
        print("Before:")
        print(_format_row(run_log_df.filter(is_run)))
        print("After:")
        print(_format_row(updated_df.filter(is_run)))
        write_run_partition(updated_df, run_number, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
