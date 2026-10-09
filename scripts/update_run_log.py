"""
Set a flag to True for one run in a local copy of the archive run log.

Works on local files only: `update_run_log.sh` copies the run log down from S3,
runs this script, and uploads the changed partition.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Sequence

import polars as pl

# Makes the repo root importable whether run directly or under pytest, where only
# this script's own directory would otherwise be on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.column_names.ind_cqc_pipeline_columns import (  # noqa: E402
    ArchiveDateRunNumberPartitionKeys as Keys,
)
from utils.column_names.ind_cqc_pipeline_columns import (  # noqa: E402
    ArchiveRunLogColumns as RunLog,
)

SUPPORTED_FLAGS: tuple[str, ...] = (RunLog.approved,)


def set_run_flag(run_log_df: pl.DataFrame, run_number: int, flag: str) -> pl.DataFrame:
    """Sets a flag to True for one run.

    Args:
        run_log_df (pl.DataFrame): The run log, including the partition key columns.
        run_number (int): The run to update.
        flag (str): The flag column to set. Only `approved` is supported.

    Returns:
        pl.DataFrame: The run log with the flag set to True for the run.

    Raises:
        ValueError: If the flag is not supported, the run number matches no row or
            more than one row, or the flag is already True for the run.
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
    if run_df[flag].item() is True:
        raise ValueError(f"Run number {run_number} is already {flag}.")

    return run_log_df.with_columns(
        pl.when(is_run).then(True).otherwise(pl.col(flag)).alias(flag)
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run_log_dir", type=Path, required=True, help="Local run log")
    parser.add_argument(
        "--output_dir", type=Path, required=True, help="Updated partition"
    )
    parser.add_argument("--run_number", type=int, required=True)
    parser.add_argument("--flag", required=True)
    return parser.parse_args(argv)


def _format_row(row_df: pl.DataFrame) -> str:
    """Formats a one-row frame as plain `column: value` lines (ASCII-safe)."""
    row = row_df.row(0, named=True)
    return "\n".join(f"  {column}: {value}" for column, value in row.items())


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Updates one run's flag and writes only that run's partition to the output dir.

    Nothing is written unless the whole update succeeds. The input directory is
    never modified.

    Args:
        argv (Optional[Sequence[str]]): Command line arguments; defaults to `sys.argv`.

    Returns:
        int: 0 on success, 1 if the update was rejected.
    """
    args = _parse_arguments(argv)

    try:
        run_log_df = read_run_log(args.run_log_dir)
        updated_df = set_run_flag(run_log_df, args.run_number, args.flag)
    except (ValueError, FileNotFoundError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    is_run = pl.col(Keys.run_number) == args.run_number
    print("Before:")
    print(_format_row(run_log_df.filter(is_run)))
    print("After:")
    print(_format_row(updated_df.filter(is_run)))

    write_run_partition(updated_df, args.run_number, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
