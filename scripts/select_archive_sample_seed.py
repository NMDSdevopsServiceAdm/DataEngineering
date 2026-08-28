"""
Decide whether a push needs to seed the branch's non-prod dataset bucket with
sample archive data.

`copy-main-data` syncs `sfc-main-datasets`' `domain=sample_archive_data` prefix
into every branch's own dataset bucket on its first deploy. This narrows that
to pushes that genuinely touch the archive stage, so an unrelated branch's
first build doesn't pay for a sync it never asked for.

Trigger paths are a fixed list rather than derived, matching
`select_raw_bucket_seed`'s approach for the same reason: there's no single
manifest that already enumerates "everything that reads the archive sample
data".
"""

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence

# Run directly (as CI does: `python scripts/select_archive_sample_seed.py`),
# only this script's own directory lands on sys.path, not the repo root -- so
# the package-style import below would fail. Adding the repo root explicitly
# makes the script runnable both that way and under pytest.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.select_bake_targets import (  # noqa: E402
    DEFAULT_DIFF_BASE,
    _normalise_path,
    changed_paths_since,
    path_triggers_rebuild,
)

# To add a new trigger path: add it here, plus a
# `returns_true_when_changed_path_is_under_<name>` case in
# scripts/tests/test_select_archive_sample_seed.py's `trigger_path_cases`.
ARCHIVE_TRIGGER_PATHS: tuple[str, ...] = (
    "projects/_03_independent_cqc/_09_archive_estimates",
    "projects/_08_publication",
)


def should_seed_archive_sample(changed_paths: Iterable[str]) -> bool:
    """
    Decide whether any changed path warrants seeding the archive sample data.

    Args:
        changed_paths (Iterable[str]): Repo-relative paths of changed files.

    Returns:
        bool: True if at least one changed path falls under a trigger path.
    """
    normalised_paths = [_normalise_path(path) for path in changed_paths]

    return any(
        path_triggers_rebuild(changed_path, trigger_path)
        for changed_path in normalised_paths
        for trigger_path in ARCHIVE_TRIGGER_PATHS
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Print "true" or "false" depending on whether this push should seed the
    archive sample data.

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

    print("true" if should_seed_archive_sample(changed_paths) else "false")
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
        description="Decide whether this push should seed the archive sample data."
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
