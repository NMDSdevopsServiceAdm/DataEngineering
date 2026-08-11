"""
Select which docker-bake targets CircleCI actually needs to build.

`task-containerisation` is around 69% of this repo's CircleCI credit spend
because it bakes all seven images on every push, whatever changed. This script
narrows that to the targets a push can genuinely affect: those whose build
context changed since the branch forked from main, plus any whose image is not
yet in ECR under this branch's tag.

The ECR half is a correctness requirement rather than an optimisation.
Terraform pulls `<ecr_repository>:<sanitised_branch>` (see
`terraform/modules/fargate-task/ecs.tf`), so skipping the build on a branch's
first push -- when no image exists under that tag yet -- would break the deploy
that follows.

Trigger paths are derived from each Dockerfile's own `COPY` sources rather than
hand-maintained, so adding a `COPY` automatically widens that target's trigger
set and the mapping cannot drift out of step with what the build consumes.
"""

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import boto3
from botocore.exceptions import ClientError

BAKE_FILE: str = "docker-bake.hcl"
DEFAULT_DIFF_BASE: str = "origin/main"

# Only a missing image means "not built for this branch yet". A missing
# repository is deliberately not treated that way: the repositories are created
# by hand in AWS and aren't per-branch, so one being absent is a setup problem,
# not a first build. Conflating the two silently rebuilt everything.
_ECR_IMAGE_ABSENT_ERROR_CODE: str = "ImageNotFoundException"
_ECR_REPOSITORY_ABSENT_ERROR_CODE: str = "RepositoryNotFoundException"

# Block bodies are matched up to a closing brace in the first column, not up to
# the first `}` -- tag values contain `${AWS_ACCOUNT_ID}` interpolations.
_GROUP_ALL_BLOCK = re.compile(r'group\s+"all"\s*\{(?P<body>.*?)\n\}', re.DOTALL)
_TARGET_BLOCK = re.compile(
    r'target\s+"(?P<name>[^"]+)"\s*\{(?P<body>.*?)\n\}', re.DOTALL
)
_QUOTED = re.compile(r'"([^"]+)"')
_DOCKERFILE_ATTRIBUTE = re.compile(r'dockerfile\s*=\s*"(?P<path>[^"]+)"')
_ECR_REPOSITORY = re.compile(r'amazonaws\.com/(?P<repository>[^:"]+):')
_HCL_COMMENT = re.compile(r"^\s*(#|//).*$", re.MULTILINE)
_LINE_CONTINUATION = re.compile(r"\\\s*\n")
_COPY_INSTRUCTION = re.compile(r"^\s*COPY\s+(?P<arguments>.+)$", re.MULTILINE)


@dataclass(frozen=True)
class BakeTarget:
    """
    A single buildable target from `docker-bake.hcl`.

    Attributes:
        name (str): Target name, as passed to `docker buildx bake`.
        dockerfile (str): Repo-relative path to the target's Dockerfile.
        ecr_repository (str): ECR repository the built image is pushed to.
        trigger_paths (tuple[str, ...]): Repo-relative paths that require a
            rebuild when changed. Derived from the Dockerfile's `COPY` sources
            plus the Dockerfile and bake file themselves.
    """

    name: str
    dockerfile: str
    ecr_repository: str
    trigger_paths: tuple[str, ...]


def parse_copy_sources(dockerfile_contents: str) -> list[str]:
    """
    Extract the source paths of every `COPY` instruction in a Dockerfile.

    A `COPY` takes one or more sources and a single destination, so every
    argument but the last is a source. Flags such as `--from=` are dropped.

    Args:
        dockerfile_contents (str): Full text of a Dockerfile.

    Returns:
        list[str]: Source paths, in the order they appear.
    """
    joined_contents = _LINE_CONTINUATION.sub(" ", dockerfile_contents)

    sources: list[str] = []
    for instruction in _COPY_INSTRUCTION.finditer(joined_contents):
        arguments = [
            argument
            for argument in instruction.group("arguments").split()
            if not argument.startswith("--")
        ]
        # A lone argument leaves no source/destination pair to read.
        if len(arguments) < 2:
            continue
        sources.extend(arguments[:-1])

    return sources


def parse_bake_targets(bake_file_contents: str) -> dict[str, tuple[str, str]]:
    """
    Read target definitions out of `docker-bake.hcl`.

    Only targets in the `all` group are returned -- that group is what
    `docker buildx bake all` builds today, so anything outside it isn't ours to
    schedule.

    Args:
        bake_file_contents (str): Full text of `docker-bake.hcl`.

    Returns:
        dict[str, tuple[str, str]]: Target name mapped to its (dockerfile path,
            ECR repository), ordered as the `all` group lists them.

    Raises:
        ValueError: If the `all` group is missing, or names a target with no
            usable dockerfile and image tag.
    """
    # Commented-out target blocks are common here; drop them before matching so
    # a disabled target isn't scheduled for a build.
    bake_file_contents = _HCL_COMMENT.sub("", bake_file_contents)

    group_match = _GROUP_ALL_BLOCK.search(bake_file_contents)
    if group_match is None:
        raise ValueError(f'No group "all" found in {BAKE_FILE}.')
    grouped_target_names = _QUOTED.findall(group_match.group("body"))

    definitions: dict[str, tuple[str, str]] = {}
    for target in _TARGET_BLOCK.finditer(bake_file_contents):
        body = target.group("body")
        dockerfile_match = _DOCKERFILE_ATTRIBUTE.search(body)
        repository_match = _ECR_REPOSITORY.search(body)
        if dockerfile_match is None or repository_match is None:
            continue
        definitions[target.group("name")] = (
            _normalise_path(dockerfile_match.group("path")),
            repository_match.group("repository"),
        )

    missing_definitions = [
        name for name in grouped_target_names if name not in definitions
    ]
    if missing_definitions:
        raise ValueError(
            f'{BAKE_FILE} group "all" names targets without a usable '
            f"dockerfile and tag: {', '.join(missing_definitions)}."
        )

    return {name: definitions[name] for name in grouped_target_names}


def load_bake_targets(repo_root: Path) -> list[BakeTarget]:
    """
    Build the target list, reading the bake file and every Dockerfile.

    Args:
        repo_root (Path): Path to the repository root.

    Returns:
        list[BakeTarget]: One entry per target in the bake file's `all` group.
    """
    definitions = parse_bake_targets((repo_root / BAKE_FILE).read_text())

    targets: list[BakeTarget] = []
    for name, (dockerfile, ecr_repository) in definitions.items():
        copy_sources = parse_copy_sources((repo_root / dockerfile).read_text())
        # The Dockerfile and bake file are build inputs in their own right.
        trigger_paths = _deduplicate(
            [_normalise_path(source) for source in copy_sources]
            + [dockerfile, BAKE_FILE]
        )
        targets.append(
            BakeTarget(
                name=name,
                dockerfile=dockerfile,
                ecr_repository=ecr_repository,
                trigger_paths=trigger_paths,
            )
        )

    return targets


def path_triggers_rebuild(changed_path: str, trigger_path: str) -> bool:
    """
    Decide whether a changed file falls inside a target's build context.

    Globbed triggers deliberately don't recurse, matching Docker's own `COPY`
    semantics: `fargate/*.py` covers that directory's files but not a
    subdirectory's.

    Args:
        changed_path (str): Repo-relative path of a changed file.
        trigger_path (str): A trigger path, which may contain a `*` glob.

    Returns:
        bool: True if the change falls within the trigger path.
    """
    if "*" in trigger_path:
        trigger_directory, _, pattern = trigger_path.rpartition("/")
        changed_directory, _, filename = changed_path.rpartition("/")
        return changed_directory == trigger_directory and fnmatch(filename, pattern)

    return changed_path == trigger_path or changed_path.startswith(f"{trigger_path}/")


def targets_with_changes(
    targets: Iterable[BakeTarget], changed_paths: Iterable[str]
) -> set[str]:
    """
    Find targets whose build context contains at least one changed file.

    Args:
        targets (Iterable[BakeTarget]): Targets to check.
        changed_paths (Iterable[str]): Repo-relative paths of changed files.

    Returns:
        set[str]: Names of the targets needing a rebuild.
    """
    normalised_paths = [_normalise_path(path) for path in changed_paths]

    return {
        target.name
        for target in targets
        for trigger_path in target.trigger_paths
        if any(
            path_triggers_rebuild(changed_path, trigger_path)
            for changed_path in normalised_paths
        )
    }


def targets_missing_from_ecr(
    targets: Iterable[BakeTarget], image_tag: str, ecr_client: Any
) -> set[str]:
    """
    Find targets with no image in ECR under this branch's tag.

    These must be built even when nothing in their build context changed --
    Terraform pulls the branch-tagged image, so a missing one breaks the deploy.

    Args:
        targets (Iterable[BakeTarget]): Targets to check.
        image_tag (str): Sanitised branch name used as the image tag.
        ecr_client (Any): A boto3 ECR client.

    Returns:
        set[str]: Names of the targets with no image under `image_tag`.

    Raises:
        RuntimeError: If a repository doesn't exist, which points at the
            credentials rather than at the repository.
        ClientError: If ECR fails for any reason other than the image being
            absent.
    """
    missing: set[str] = set()
    for target in targets:
        try:
            ecr_client.describe_images(
                repositoryName=target.ecr_repository,
                imageIds=[{"imageTag": image_tag}],
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == _ECR_REPOSITORY_ABSENT_ERROR_CODE:
                raise RuntimeError(
                    f"ECR repository {target.ecr_repository!r} not found. These "
                    "repositories are created by hand in AWS and aren't "
                    "per-branch, so either these credentials are for the wrong "
                    "account, or a new bake target has been added without "
                    "creating its repository."
                ) from error
            if error_code != _ECR_IMAGE_ABSENT_ERROR_CODE:
                raise
            missing.add(target.name)

    return missing


def select_targets(
    targets: Sequence[BakeTarget],
    changed_paths: Iterable[str],
    image_tag: Optional[str] = None,
    ecr_client: Optional[Any] = None,
) -> list[str]:
    """
    Select the targets to bake for this push.

    Args:
        targets (Sequence[BakeTarget]): All buildable targets.
        changed_paths (Iterable[str]): Repo-relative paths of changed files.
        image_tag (Optional[str]): Sanitised branch name used as the image tag.
            Only read when `ecr_client` is given.
        ecr_client (Optional[Any]): A boto3 ECR client, or None to skip the
            missing-image check. Skipping is only safe for local dry runs.

    Returns:
        list[str]: Target names to bake, in bake-file order. Empty means the
            build can be skipped entirely.
    """
    # Materialised because it's logged as well as iterated.
    changed_paths = list(changed_paths)

    changed_targets = targets_with_changes(targets, changed_paths)
    _log(f"changed files: {sorted(changed_paths)}")
    _log(f"targets with changes: {sorted(changed_targets)}")

    missing_targets: set[str] = set()
    if ecr_client is not None:
        _log(
            "querying ECR repositories: "
            f"{sorted(target.ecr_repository for target in targets)}"
        )
        missing_targets = targets_missing_from_ecr(targets, image_tag, ecr_client)
        _log(f"targets with no {image_tag!r} image in ECR: {sorted(missing_targets)}")

    selected = changed_targets | missing_targets

    return [target.name for target in targets if target.name in selected]


def changed_paths_since(diff_base: str, repo_root: Path) -> list[str]:
    """
    List files changed since the branch forked from `diff_base`.

    The three-dot form diffs against the merge base rather than the tip of
    `diff_base`, so the result is the branch's own changes. That keeps the
    decision stateless -- a failed or cancelled earlier build can't leave a
    stale image behind.

    Args:
        diff_base (str): Ref the branch forked from, e.g. `origin/main`.
        repo_root (Path): Path to the repository root.

    Returns:
        list[str]: Repo-relative paths of the changed files.
    """
    completed = subprocess.run(
        ["git", "diff", "--name-only", f"{diff_base}...HEAD"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )

    return completed.stdout.splitlines()


def _log(message: str) -> None:
    """
    Write a diagnostic line to stderr.

    Stderr specifically: stdout carries the target list, which CI redirects
    into the workspace file.

    Args:
        message (str): The line to write.
    """
    print(message, file=sys.stderr)


def _normalise_path(path: str) -> str:
    """
    Normalise a path to the repo-relative, forward-slashed form git reports.

    Args:
        path (str): A path from the bake file, a Dockerfile, or the CLI.

    Returns:
        str: The path without a leading `./` and with forward slashes.
    """
    return path.replace("\\", "/").removeprefix("./")


def _deduplicate(paths: Iterable[str]) -> tuple[str, ...]:
    """
    Drop repeated paths while preserving first-seen order.

    Args:
        paths (Iterable[str]): Paths to deduplicate.

    Returns:
        tuple[str, ...]: The paths, deduplicated.
    """
    return tuple(dict.fromkeys(paths))


def _parse_arguments(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    """
    Parse command line arguments.

    Args:
        argv (Optional[Sequence[str]]): Argument list, or None to read sys.argv.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Select which docker-bake targets CircleCI needs to build."
    )
    parser.add_argument(
        "--image-tag",
        default=os.environ.get("SANITISED_CIRCLE_BRANCH"),
        help="Image tag to look for in ECR. Defaults to $SANITISED_CIRCLE_BRANCH.",
    )
    parser.add_argument(
        "--diff-base",
        default=DEFAULT_DIFF_BASE,
        help=f"Ref the branch forked from. Defaults to {DEFAULT_DIFF_BASE}.",
    )
    parser.add_argument(
        "--region",
        # Passed to boto3 explicitly rather than left to the environment:
        # botocore reads AWS_DEFAULT_REGION, not the AWS_REGION the rest of this
        # repo's CI config sets.
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="AWS region holding the ECR repositories. Defaults to $AWS_REGION.",
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
    parser.add_argument(
        "--skip-ecr-check",
        action="store_true",
        help=(
            "Skip the missing-image check. Dry runs only -- without it the "
            "result can wrongly skip a branch's first build."
        ),
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    Print the space-separated bake targets for this push.

    Args:
        argv (Optional[Sequence[str]]): Argument list, or None to read sys.argv.

    Returns:
        int: Process exit code.
    """
    arguments = _parse_arguments(argv)
    if not arguments.skip_ecr_check:
        missing_arguments = [
            name
            for name, value in [
                ("--image-tag", arguments.image_tag),
                ("--region", arguments.region),
            ]
            if value is None
        ]
        if missing_arguments:
            print(
                f"{', '.join(missing_arguments)} (or the matching environment "
                "variable) is required unless --skip-ecr-check is passed.",
                file=sys.stderr,
            )
            return 2

    targets = load_bake_targets(arguments.repo_root)
    changed_paths = (
        arguments.changed_paths
        if arguments.changed_paths is not None
        else changed_paths_since(arguments.diff_base, arguments.repo_root)
    )
    ecr_client = (
        None
        if arguments.skip_ecr_check
        else boto3.client("ecr", region_name=arguments.region)
    )
    selected = select_targets(
        targets,
        changed_paths,
        image_tag=arguments.image_tag,
        ecr_client=ecr_client,
    )

    print(" ".join(selected))
    return 0


if __name__ == "__main__":
    sys.exit(main())
