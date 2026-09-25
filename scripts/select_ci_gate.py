"""
Decide whether a push touches the paths behind one CI gate, printing "true" or
"false" for `decide-bake-and-seed` to hand on to the job it gates.

Each gate narrows an expensive or side-effecting dev-branch step -- seeding the
branch's raw or dataset bucket, or running the live CQC API integration tests
-- to pushes that genuinely touch it, so an unrelated branch doesn't pay for
it. Main never consults these gates: a merge-base diff against main is empty by
definition there.

Trigger paths are fixed lists rather than derived (unlike
`select_bake_targets`), because no single manifest already enumerates
"everything that reads this data".
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

# Cross-cutting raw bucket trigger -- a change here seeds every domain.
SHARED_RAW_BUCKET_TRIGGER_PATHS: tuple[str, ...] = (
    "terraform/pipeline/eventbridge.tf",
)

# To add a gate: add an entry here, plus its trigger and unrelated-path test cases.
GATE_TRIGGER_PATHS: dict[str, tuple[str, ...]] = {
    # Split per domain so a push touching one doesn't reseed and re-trigger the
    # others' Step Functions. `cqc_api` has no gate: it reads the CQC API, not
    # the raw bucket.
    "raw-bucket-ascwds": (
        "projects/_01_ingest/ascwds/fargate/ingest_ascwds_dataset.py",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_worker_raw_data.py",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_workplace_raw_data.py",
        *SHARED_RAW_BUCKET_TRIGGER_PATHS,
    ),
    # Just the ingest job: capacity_tracker has no raw-validate job.
    "raw-bucket-capacity_tracker": (
        "projects/_01_ingest/capacity_tracker/fargate/ingest_capacity_tracker_data.py",
        *SHARED_RAW_BUCKET_TRIGGER_PATHS,
    ),
    "raw-bucket-cqc_pir": (
        "projects/_01_ingest/cqc_pir/fargate/ingest_cqc_pir_data.py",
        "projects/_01_ingest/cqc_pir/fargate/validate_cqc_pir_raw_data.py",
        "projects/_01_ingest/cqc_pir/fargate/clean_cqc_pir_data.py",
        "projects/_01_ingest/cqc_pir/fargate/validate_clean_cqc_pir_data.py",
        *SHARED_RAW_BUCKET_TRIGGER_PATHS,
    ),
    "raw-bucket-ons_pd": (
        "projects/_01_ingest/ons_pd/fargate/ingest_ons_data.py",
        "projects/_01_ingest/ons_pd/fargate/validate_postcode_directory_raw_data.py",
        *SHARED_RAW_BUCKET_TRIGGER_PATHS,
    ),
    # Seeds the branch's dataset bucket with `sfc-main-datasets`' job role
    # archive datasets.
    "archive-sample": (
        "projects/_03_independent_cqc/_01_filled_posts/_07_archive",
        "projects/_99_publication",
    ),
    # Anything that could change the live CQC API tests' behaviour, including
    # the shared secrets helper that fetches the API key.
    "cqc-integration-tests": (
        "projects/_01_ingest/cqc_api",
        "tests/integration/test_cqc_api_integration.py",
        "utils/column_names/raw_data_files/cqc_location_api_columns.py",
        "utils/column_names/raw_data_files/cqc_provider_api_columns.py",
        "utils/aws_secrets_manager_utilities.py",
    ),
}


def gate_triggered(gate: str, changed_paths: Iterable[str]) -> bool:
    """
    Decide whether any changed path falls under one gate's trigger paths.

    Args:
        gate (str): Gate name, a key of GATE_TRIGGER_PATHS.
        changed_paths (Iterable[str]): Repo-relative paths of changed files.

    Returns:
        bool: True if at least one changed path falls under a trigger path.
    """
    normalised_paths = [_normalise_path(path) for path in changed_paths]

    return any(
        path_triggers_rebuild(changed_path, trigger_path)
        for changed_path in normalised_paths
        for trigger_path in GATE_TRIGGER_PATHS[gate]
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Print "true" or "false" depending on whether this push triggers the given
    gate.

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

    print("true" if gate_triggered(arguments.gate, changed_paths) else "false")
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
        description="Decide whether this push triggers one CI gate."
    )
    parser.add_argument(
        "--gate",
        required=True,
        choices=list(GATE_TRIGGER_PATHS),
        help="Gate to decide the flag for.",
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
