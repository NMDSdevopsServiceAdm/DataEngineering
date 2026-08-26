"""
Decide, per ingest domain, whether a push needs to seed that domain's slice of
the branch's non-prod raw data bucket.

Deciding per domain -- rather than one bucket-wide flag -- means a push that
only touches one domain's ingest code doesn't reseed (and re-trigger) the
other domains' Step Functions.

Trigger paths are a fixed list rather than derived, unlike
`select_bake_targets`'s Dockerfile-driven approach -- there's no manifest that
already enumerates "everything that reads the raw bucket".
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

# Specific files that read/write/validate each domain's raw data -- not whole
# domain directories, which also matched tests and downstream
# clean/validate-cleaned jobs. `cqc_api` reads the CQC API directly, so it's
# excluded; `capacity_tracker` has no raw-validate job here, so it only gets
# its ingest entrypoint. Paths use `fargate/` uniformly (matching each
# domain's migration target) rather than each domain's current location.
#
# To add a new trigger path: add it here, plus a matching case in
# scripts/tests/test_select_raw_bucket_seed.py's `domain_trigger_cases`.
DOMAIN_TRIGGER_PATHS: dict[str, tuple[str, ...]] = {
    "ascwds": (
        "projects/_01_ingest/ascwds/fargate/ingest_ascwds_dataset.py",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_worker_raw_data.py",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_workplace_raw_data.py",
    ),
    "capacity_tracker": (
        "projects/_01_ingest/capacity_tracker/fargate/ingest_capacity_tracker_data.py",
    ),
    "cqc_pir": (
        "projects/_01_ingest/cqc_pir/fargate/ingest_cqc_pir_data.py",
        "projects/_01_ingest/cqc_pir/fargate/validate_cqc_pir_raw_data.py",
    ),
    "ons_pd": (
        "projects/_01_ingest/ons_pd/fargate/ingest_ons_data.py",
        "projects/_01_ingest/ons_pd/fargate/validate_postcode_directory_raw_data.py",
    ),
}

# Cross-cutting trigger -- a change here seeds every domain.
SHARED_TRIGGER_PATHS: tuple[str, ...] = ("terraform/pipeline/eventbridge.tf",)


def should_seed_domain(domain: str, changed_paths: Iterable[str]) -> bool:
    """
    Decide whether any changed path warrants seeding one domain's raw data.

    Args:
        domain (str): Domain name, a key of DOMAIN_TRIGGER_PATHS.
        changed_paths (Iterable[str]): Repo-relative paths of changed files.

    Returns:
        bool: True if at least one changed path falls under that domain's
            trigger paths or a shared trigger path.
    """
    normalised_paths = [_normalise_path(path) for path in changed_paths]
    trigger_paths = DOMAIN_TRIGGER_PATHS[domain] + SHARED_TRIGGER_PATHS

    return any(
        path_triggers_rebuild(changed_path, trigger_path)
        for changed_path in normalised_paths
        for trigger_path in trigger_paths
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Print "true" or "false" depending on whether this push should seed the
    given domain's raw data.

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

    print("true" if should_seed_domain(arguments.domain, changed_paths) else "false")
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
        description="Decide whether this push should seed one domain's non-prod raw data."
    )
    parser.add_argument(
        "--domain",
        required=True,
        choices=list(DOMAIN_TRIGGER_PATHS),
        help="Ingest domain to decide the seed flag for.",
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
