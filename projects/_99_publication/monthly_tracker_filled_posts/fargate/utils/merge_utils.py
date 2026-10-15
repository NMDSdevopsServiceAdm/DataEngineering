from utils.file_utils import RUN_PARTITION_PATTERN, list_archive_runs

LATEST_RUN = "latest"


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


def parse_required_run_number(value: str) -> int:
    """
    Converts the run_number command line value into a run number.

    Args:
        value (str): a whole number

    Returns:
        int: the run number

    Raises:
        ValueError: if the value is not a whole number
    """
    if not value.isdecimal():
        raise ValueError(f"run_number must be a whole number, got '{value}'")
    return int(value)


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


def run_partition_path(s3_root: str, run_source: str) -> str:
    """
    Builds a run's directory under an S3 root from the run's source directory.

    Args:
        s3_root (str): S3 directory holding runs partitioned by archive_date and
            run_number
        run_source (str): S3 directory of a run, from resolve_run_sources

    Returns:
        str: the run's directory under s3_root

    Raises:
        ValueError: if run_source has no archive_date and run_number partitions
    """
    partition = RUN_PARTITION_PATTERN.search(run_source)
    if partition is None:
        raise ValueError(f"No run partition found in {run_source}")

    return f"{s3_root.rstrip('/')}/{partition.group(0)}"


def run_number_from_source(run_source: str) -> int:
    """
    Gets the run number from a run's source directory.

    Args:
        run_source (str): S3 directory of a run, from resolve_run_sources

    Returns:
        int: the run number

    Raises:
        ValueError: if run_source has no archive_date and run_number partitions
    """
    partition = RUN_PARTITION_PATTERN.search(run_source)
    if partition is None:
        raise ValueError(f"No run partition found in {run_source}")

    return int(partition.group(2))
