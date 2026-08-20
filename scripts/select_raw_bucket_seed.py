"""
Decide whether a push needs to seed the branch's non-prod raw data bucket.

`copy-main-data` syncs `sfc-main-sample-raw-data` into every branch's own raw
bucket on its first deploy, which fires S3 "Object Created" events and
triggers all five ingest Step Functions (`terraform/pipeline/eventbridge.tf`)
regardless of what the branch is actually about. This narrows that to pushes
that genuinely touch ingest-related code or the raw bucket's own
infrastructure, so an unrelated branch's first build doesn't burn Step
Functions/Glue/Fargate runs on data it never asked for.

Trigger paths are a fixed list rather than derived, unlike
`select_bake_targets`'s Dockerfile-driven approach -- there's no single
manifest that already enumerates "everything that reads the raw bucket".
"""

import argparse
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence

# Run directly (as CI does: `python scripts/select_raw_bucket_seed.py`), only
# this script's own directory lands on sys.path, not the repo root -- so the
# package-style import below would fail. Adding the repo root explicitly
# makes the script runnable both that way and under pytest.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.select_bake_targets import (  # noqa: E402
    DEFAULT_DIFF_BASE,
    _normalise_path,
    changed_paths_since,
    path_triggers_rebuild,
)

# Ingest domains that actually read from the raw bucket. `cqc_api` is a
# sibling ingest domain that reads from the CQC API directly, not the raw
# bucket, so it's deliberately excluded.
INGEST_TRIGGER_PATHS: tuple[str, ...] = (
    "projects/_01_ingest/ascwds",
    "projects/_01_ingest/capacity_tracker",
    "projects/_01_ingest/cqc_pir",
    "projects/_01_ingest/ons_pd",
    "terraform/pipeline/eventbridge.tf",
    "terraform/pipeline/iam.tf",
    "terraform/pipeline/s3.tf",
    "terraform/modules/fargate-task",
    ".circleci/config.yml",
)


def should_seed_raw_bucket(changed_paths: Iterable[str]) -> bool:
    """
    Decide whether any changed path warrants seeding the raw bucket.

    Args:
        changed_paths (Iterable[str]): Repo-relative paths of changed files.

    Returns:
        bool: True if at least one changed path falls under a trigger path.
    """
    normalised_paths = [_normalise_path(path) for path in changed_paths]

    return any(
        path_triggers_rebuild(changed_path, trigger_path)
        for changed_path in normalised_paths
        for trigger_path in INGEST_TRIGGER_PATHS
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Print "true" or "false" depending on whether this push should seed the
    raw bucket.

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

    print("true" if should_seed_raw_bucket(changed_paths) else "false")
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
        description="Decide whether this push should seed the non-prod raw bucket."
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
