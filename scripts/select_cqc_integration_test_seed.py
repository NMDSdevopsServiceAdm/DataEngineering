"""
Decide whether a push touches CQC ingestion code closely enough to warrant
running the live CQC API integration tests on a dev branch. Main always runs
them, since a merge-base diff against main is empty by definition there.
"""

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence

# Makes the repo root importable whether run directly (as CI does) or under
# pytest, where only this script's own directory would otherwise be on the path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.select_bake_targets import (  # noqa: E402
    DEFAULT_DIFF_BASE,
    _normalise_path,
    changed_paths_since,
    path_triggers_rebuild,
)

# Anything that could plausibly change the CQC integration tests' behaviour:
# the client itself, the test file, its column-name dependencies, and the
# shared secrets helper it uses to fetch the API key.
TRIGGER_PATHS: tuple[str, ...] = (
    "projects/_01_ingest/cqc_api",
    "tests/integration/test_cqc_api_integration.py",
    "utils/column_names/raw_data_files/cqc_location_api_columns.py",
    "utils/column_names/raw_data_files/cqc_provider_api_columns.py",
    "utils/aws_secrets_manager_utilities.py",
)


def should_run_cqc_integration_tests(changed_paths: Iterable[str]) -> bool:
    """
    Decide whether any changed path warrants running the CQC integration tests.

    Args:
        changed_paths (Iterable[str]): Repo-relative paths of changed files.

    Returns:
        bool: True if at least one changed path falls under a trigger path.
    """
    normalised_paths = [_normalise_path(path) for path in changed_paths]

    return any(
        path_triggers_rebuild(changed_path, trigger_path)
        for changed_path in normalised_paths
        for trigger_path in TRIGGER_PATHS
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Print "true" or "false" depending on whether this push should run the
    CQC API integration tests.

    Args:
        argv (Optional[Sequence[str]]): Argument list, or None to read sys.argv.

    Returns:
        int: Process exit code.
    """
    arguments = _parse_arguments(argv)
    changed_paths = (
        arguments.changed_paths
        if arguments.changed_paths is not None
        else changed_paths_since(arguments.diff_base, arguments.repo_root)
    )

    print("true" if should_run_cqc_integration_tests(changed_paths) else "false")
    return 0


def _parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse command line arguments.

    Args:
        argv (Optional[Sequence[str]]): Argument list, or None to read sys.argv.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Decide whether this push should run the CQC API integration tests."
    )
    parser.add_argument(
        "--diff-base",
        default=DEFAULT_DIFF_BASE,
        help=f"Ref the branch forked from. Defaults to {DEFAULT_DIFF_BASE}.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="Repository root. Defaults to the parent of this script's directory.",
    )
    parser.add_argument(
        "--changed-path",
        action="append",
        dest="changed_paths",
        help=(
            "Treat this path as changed instead of asking git. Repeatable, and "
            "intended for dry runs."
        ),
    )

    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
