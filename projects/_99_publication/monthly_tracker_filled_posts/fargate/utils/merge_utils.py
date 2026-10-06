import re

import boto3

from utils.file_utils import split_s3_uri

LATEST_RUN = "latest"
RUN_PARTITION_PATTERN = re.compile(
    r"archive_date=(\d{4}-\d{2}-\d{2})/run_number=(\d+)/"
)


def parse_run_number(value: str | None) -> int | None:
    """
    Converts the run_number command line value into a run number.

    Args:
        value (str | None): a run number, "latest", or None when not given

    Returns:
        int | None: the run number, or None for the latest run

    Raises:
        ValueError: if the value is neither "latest" nor a whole number
    """
    if value is None or value == LATEST_RUN:
        return None
    if not value.isdecimal():
        raise ValueError(
            f"run_number must be a whole number or 'latest', got '{value}'"
        )
    return int(value)


def list_archive_runs(s3_root: str) -> dict[int, str]:
    """
    Lists the archived runs under an S3 archive root.

    Reads the archive_date and run_number partitions from the object keys, e.g.
    <root>/archive_date=2026-09-22/run_number=1/file.parquet

    Args:
        s3_root (str): S3 directory of an archive, partitioned by archive_date and
            run_number

    Returns:
        dict[int, str]: the archive_date (yyyy-mm-dd) of each run_number found
    """
    bucket, prefix = split_s3_uri(s3_root.rstrip("/") + "/")
    pages = (
        boto3.client("s3")
        .get_paginator("list_objects_v2")
        .paginate(Bucket=bucket, Prefix=prefix)
    )

    return {
        int(match.group(2)): match.group(1)
        for page in pages
        for obj in page.get("Contents", [])
        if (match := RUN_PARTITION_PATTERN.search(obj["Key"]))
    }


def select_run_number(
    runs_by_root: dict[str, dict[int, str]], run_number: int | None
) -> int:
    """
    Chooses the run to use from the runs archived under each S3 root.

    The run must exist under every root. When no run_number is requested, the highest
    run_number is used, which must be the same under every root.

    Args:
        runs_by_root (dict[str, dict[int, str]]): the archive_date of each run_number,
            for each S3 root
        run_number (int | None): the requested run_number, or None for the latest run

    Returns:
        int: the run_number to use

    Raises:
        ValueError: if a root has no runs, the requested run is missing from a root,
            or the roots disagree on the latest run
    """
    for root, runs in runs_by_root.items():
        if not runs:
            raise ValueError(f"No archived runs found in {root}")
        if run_number is not None and run_number not in runs:
            raise ValueError(
                f"run_number {run_number} not found in {root}. Available runs: {sorted(runs)}"
            )

    if run_number is not None:
        return run_number

    latest_by_root = {root: max(runs) for root, runs in runs_by_root.items()}
    if len(set(latest_by_root.values())) > 1:
        raise ValueError(f"The latest run differs between archives: {latest_by_root}")

    return next(iter(latest_by_root.values()))


def resolve_run_sources(s3_roots: list[str], run_number: int | None) -> list[str]:
    """
    Finds the S3 directory of one archived run under each of the given archive roots.

    Args:
        s3_roots (list[str]): S3 directories of archives partitioned by archive_date
            and run_number, which share their run numbers
        run_number (int | None): the run to use, or None for the latest run

    Returns:
        list[str]: the S3 directory of the selected run under each root, in order
    """
    runs_by_root = {root: list_archive_runs(root) for root in s3_roots}
    selected_run = select_run_number(runs_by_root, run_number)

    return [
        f"{root.rstrip('/')}/archive_date={runs[selected_run]}/run_number={selected_run}/"
        for root, runs in runs_by_root.items()
    ]
