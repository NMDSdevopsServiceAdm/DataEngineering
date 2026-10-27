"""
Merge `ready-to-merge` PRs one at a time, in label order, once the deploy of
main's tip has finished: a PR merged mid-deploy fails at prod terraform.

Needs GH_TOKEN (for `gh`) and CIRCLECI_TOKEN.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from enum import Enum
from typing import Callable, Collection, Optional, TypeVar

T = TypeVar("T")

CIRCLECI_API = "https://circleci.com/api/v2"
MAIN_DEPLOY_WORKFLOW = "test-plan-approve-and-deploy-to-main"
PIPELINES_TO_SCAN = 20
FAILED_STATUSES = {"failed", "failing", "error", "canceled", "unauthorized"}
FAILED_BUCKETS = {"fail", "cancel"}
VERDICT_REVIEW_STATES = {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}
# This workflow's own jobs: ignored so the script doesn't wait on itself.
OWN_CHECK_NAMES = {"merge-when-ready", "announce-queue-position"}
POLL_SECONDS = 60
# Time for CircleCI to start a pipeline after a push or merge.
APPEARANCE_TIMEOUT_SECONDS = 600
PR_WORKFLOWS_TIMEOUT_SECONDS = 7200
MAX_ROUNDS = 5
READ_ATTEMPTS = 3
RETRY_SECONDS = 15
MAX_DETAIL_CHARS = 300
ERRORS = (
    subprocess.CalledProcessError,
    urllib.error.URLError,
    TimeoutError,
    json.JSONDecodeError,
)


class State(str, Enum):
    CLEAR = "clear"
    PENDING = "pending"
    FAILED = "failed"


class NotMergeable(Exception):
    """A PR can't be merged; the message says why and is posted on the PR."""


class MainDeployFailed(Exception):
    """Main's deploy failed or is missing, so no PR can merge until it is fixed."""


def workflows_state(statuses: Optional[list[str]]) -> State:
    """FAILED if any failed, CLEAR if all succeeded, else PENDING (incl. none found)."""
    if not statuses:
        return State.PENDING
    if any(status in FAILED_STATUSES for status in statuses):
        return State.FAILED
    if all(status == "success" for status in statuses):
        return State.CLEAR
    return State.PENDING


def checks_state(checks: list[dict]) -> State:
    """FAILED if any check failed, PENDING if any is running or none reported, else CLEAR.

    This workflow's own jobs are ignored.
    """
    buckets = [c["bucket"] for c in checks if c["name"] not in OWN_CHECK_NAMES]
    if any(bucket in FAILED_BUCKETS for bucket in buckets):
        return State.FAILED
    if not buckets or "pending" in buckets:
        return State.PENDING
    return State.CLEAR


def commit_workflow_statuses(
    pipelines: list[dict],
    sha: str,
    get_workflows: Callable[[str], list[dict]],
    workflow_name: Optional[str] = None,
) -> Optional[list[str]]:
    """Return workflow statuses from the newest pipeline run for a commit.

    Args:
        pipelines (list[dict]): CircleCI pipelines, newest first.
        sha (str): Commit the pipeline ran for.
        get_workflows (Callable[[str], list[dict]]): Pipeline id to its workflows.
        workflow_name (Optional[str]): Only count workflows with this name.

    Returns:
        Optional[list[str]]: Statuses, or None if no matching workflow exists yet.
    """
    for pipeline in pipelines:
        if pipeline.get("vcs", {}).get("revision") != sha:
            continue
        statuses = [
            w["status"]
            for w in get_workflows(pipeline["id"])
            if workflow_name in (None, w["name"])
        ]
        if statuses:
            return statuses
    return None


def labelled_at(events: list[dict], label: str) -> str:
    """Return when `label` was last added, or "" if unknown."""
    return max(
        (
            e["created_at"]
            for e in events
            if e.get("event") == "labeled" and e.get("label", {}).get("name") == label
        ),
        default="",
    )


def _queue_key(pr: dict) -> tuple[str, int]:
    return (pr["labelled_at"] or "~", pr["number"])


def next_in_queue(prs: list[dict], handled: Collection[int] = ()) -> Optional[dict]:
    """Return the earliest labelled PR not in `handled` (unknown times last), or None."""
    return min(
        (pr for pr in prs if pr["number"] not in handled),
        key=_queue_key,
        default=None,
    )


def queue_position(prs: list[dict], number: int) -> Optional[int]:
    """Return a PR's 1-based place in label order, or None if it isn't queued."""
    ordered = [pr["number"] for pr in sorted(prs, key=_queue_key)]
    return ordered.index(number) + 1 if number in ordered else None


def is_approved(reviews: list[dict]) -> bool:
    """Return whether, by each reviewer's latest verdict, one approves and none requests changes."""
    # Comments don't change a verdict.
    latest: dict[str, str] = {}
    for review in sorted(reviews, key=lambda r: r["submittedAt"]):
        if review["state"] in VERDICT_REVIEW_STATES:
            latest[review["author"]["login"]] = review["state"]
    verdicts = set(latest.values())
    return "APPROVED" in verdicts and "CHANGES_REQUESTED" not in verdicts


def _describe(error: Exception) -> str:
    detail = getattr(error, "stderr", None) or str(error)
    return detail.strip()[:MAX_DETAIL_CHARS]


def _retry(call: Callable[[], T]) -> T:
    """Retry a read-only call, so a brief outage doesn't reject a PR."""
    for attempt in range(1, READ_ATTEMPTS + 1):
        try:
            return call()
        except ERRORS as error:
            if attempt == READ_ATTEMPTS:
                raise
            print(f"Retrying after error: {_describe(error)}")
            time.sleep(RETRY_SECONDS * attempt)
    raise AssertionError("unreachable")


def _circleci_get(path: str) -> dict:
    def fetch() -> dict:
        request = urllib.request.Request(
            f"{CIRCLECI_API}{path}",
            headers={"Circle-Token": os.environ["CIRCLECI_TOKEN"]},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    return _retry(fetch)


def _statuses_for_commit(
    project_slug: str, branch: str, sha: str, workflow_name: Optional[str] = None
) -> Optional[list[str]]:
    branch_path = urllib.parse.quote(branch, safe="")
    pipelines = _circleci_get(f"/project/{project_slug}/pipeline?branch={branch_path}")
    return commit_workflow_statuses(
        pipelines["items"][:PIPELINES_TO_SCAN],
        sha,
        lambda pipeline_id: _circleci_get(f"/pipeline/{pipeline_id}/workflow")["items"],
        workflow_name,
    )


def _gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=check)


def _gh_json(*args: str):
    return _retry(lambda: json.loads(_gh(*args).stdout))


def _label_events(repo: str, number: int) -> list[dict]:
    pages = _gh_json(
        "api", "--paginate", "--slurp", f"repos/{repo}/issues/{number}/events"
    )
    return [event for page in pages for event in page]


def _labelled_prs(repo: str, label: str) -> list[dict]:
    prs = _gh_json(
        "pr", "list", "--repo", repo, "--label", label, "--state", "open",
        "--json", "number",
    )  # fmt: skip
    for pr in prs:
        pr["labelled_at"] = labelled_at(_label_events(repo, pr["number"]), label)
    return prs


def _pr_head(repo: str, number: int) -> tuple[str, str]:
    pr = _gh_json(
        "pr", "view", str(number), "--repo", repo, "--json", "headRefOid,headRefName"
    )
    return pr["headRefOid"], pr["headRefName"]


def _require_approval(repo: str, number: int) -> None:
    reviews = _gh_json("pr", "view", str(number), "--repo", repo, "--json", "reviews")
    if not is_approved(reviews["reviews"]):
        raise NotMergeable("It needs an approving review. Re-add the label once approved.")  # fmt: skip


def _main_tip(repo: str) -> str:
    path = f"repos/{repo}/commits/main"
    return _retry(lambda: _gh("api", path, "--jq", ".sha")).stdout.strip()


def _behind_by(repo: str, head_sha: str) -> int:
    path = f"repos/{repo}/compare/main...{head_sha}"
    return int(_retry(lambda: _gh("api", path, "--jq", ".behind_by")).stdout)


def _pr_checks(repo: str, number: int) -> list[dict]:
    def fetch() -> list[dict]:
        # gh exits non-zero while checks are pending or none exist: read stdout only.
        out = _gh(
            "pr", "checks", str(number), "--repo", repo, "--json", "name,bucket",
            check=False,
        ).stdout  # fmt: skip
        return json.loads(out) if out.strip() else []

    return _retry(fetch)


def _remove_label(repo: str, label: str, number: int) -> None:
    # Best effort: a merged PR must never be reported as not merged.
    try:
        _gh("pr", "edit", str(number), "--repo", repo, "--remove-label", label)
    except subprocess.CalledProcessError as error:
        print(f"Could not remove label from PR {number}: {_describe(error)}")


def _reject(repo: str, label: str, number: int, reason: str) -> None:
    _remove_label(repo, label, number)
    try:
        _gh("pr", "comment", str(number), "--repo", repo, "--body", reason)
    except subprocess.CalledProcessError as error:
        print(f"Could not comment on PR {number}: {_describe(error)}")
    print(f"PR {number} left the queue: {reason}")


def announce_queued(repo: str, label: str, number: int) -> None:
    """Comment on a PR with its place in the queue, and a warning if unapproved."""
    prs = _labelled_prs(repo, label)
    position = queue_position(prs, number)
    if position is None:  # label already removed
        return
    message = (
        f"Queued: position {position} of {len(prs)}. "
        "PRs merge in the order they were labelled."
    )
    try:
        _require_approval(repo, number)
    except NotMergeable as error:
        message += f" Note: {error}"
    _gh("pr", "comment", str(number), "--repo", repo, "--body", message)
    print(message)


def _wait_while_pending(
    description: str, get_state: Callable[[], State], timeout: Optional[int]
) -> Optional[State]:
    """Poll until not PENDING; None if `timeout` passes first."""
    waited = 0
    while (state := get_state()) is State.PENDING:
        if timeout is not None and waited >= timeout:
            return None
        print(f"Waiting: {description}.")
        time.sleep(POLL_SECONDS)
        waited += POLL_SECONDS
    return state


def _wait_for_workflows(
    description: str,
    fetch: Callable[[], Optional[list[str]]],
    finish_timeout: Optional[int],
) -> Optional[State]:
    """Wait for CircleCI workflows to appear, then finish.

    Args:
        description (str): What is waited for, for the log.
        fetch (Callable[[], Optional[list[str]]]): Current statuses, or None.
        finish_timeout (Optional[int]): Seconds allowed to finish; None waits
            indefinitely (manual approvals).

    Returns:
        Optional[State]: CLEAR or FAILED, or None if none appeared in time.

    Raises:
        TimeoutError: If the workflows appeared but didn't finish in time.
    """
    appeared = _wait_while_pending(
        f"{description} (waiting to start)",
        lambda: State.PENDING if fetch() is None else State.CLEAR,
        APPEARANCE_TIMEOUT_SECONDS,
    )
    if appeared is None:
        return None
    state = _wait_while_pending(
        description, lambda: workflows_state(fetch()), finish_timeout
    )
    if state is None:
        raise TimeoutError(f"{description} did not finish in time.")
    return state


def _merge_pr(
    repo: str, label: str, number: int, project_slug: str, dry_run: bool
) -> None:
    """Update a PR from main, wait for its checks and main's deploy, then merge it.

    Args:
        repo (str): `owner/name`.
        label (str): Ready-to-merge label, removed once done.
        number (int): PR number.
        project_slug (str): CircleCI project slug, e.g. `gh/owner/name`.
        dry_run (bool): Run every check but don't merge.

    Raises:
        NotMergeable: If the PR isn't approved, its checks fail or main keeps moving.
        MainDeployFailed: If main's tip has no deploy or its deploy failed.
    """
    for _ in range(MAX_ROUNDS):
        _require_approval(repo, number)
        head_sha, head_branch = _pr_head(repo, number)
        if _behind_by(repo, head_sha) > 0:
            print(f"PR {number} is behind main; updating its branch.")
            _gh("api", "-X", "PUT", f"repos/{repo}/pulls/{number}/update-branch")
            time.sleep(POLL_SECONDS)  # let CircleCI see the new head
            continue

        checks = _wait_while_pending(
            f"PR {number} checks",
            lambda: checks_state(_pr_checks(repo, number)),
            PR_WORKFLOWS_TIMEOUT_SECONDS,
        )
        if checks is None:
            raise NotMergeable("Its checks didn't finish in time.")
        if checks is State.FAILED:
            raise NotMergeable("Checks failed. Fix them and re-add the label.")

        # Checks can report before every CircleCI workflow has finished.
        circleci = _wait_for_workflows(
            f"CircleCI workflows for PR {number}",
            lambda: _statuses_for_commit(project_slug, head_branch, head_sha),
            PR_WORKFLOWS_TIMEOUT_SECONDS,
        )
        if circleci is None:
            raise NotMergeable("CircleCI never started checks on its latest commit.")
        if circleci is State.FAILED:
            raise NotMergeable("CircleCI workflows failed. Fix them and re-add the label.")  # fmt: skip

        # Main's tip, not the latest deploy found: right after a merge its
        # pipeline may not exist yet.
        tip = _main_tip(repo)
        deploy = _wait_for_workflows(
            f"Main deploy of {tip[:8]}",
            lambda: _statuses_for_commit(
                project_slug, "main", tip, MAIN_DEPLOY_WORKFLOW
            ),
            None,
        )
        if deploy is None:
            raise MainDeployFailed(f"No main deploy found for {tip[:8]}.")
        if deploy is State.FAILED:
            raise MainDeployFailed(f"The main deploy of {tip[:8]} failed.")

        if _behind_by(repo, head_sha) == 0:  # main may have moved meanwhile
            break
    else:
        raise NotMergeable("Main kept moving past this PR. Re-add the label to retry.")

    _require_approval(repo, number)  # may have been withdrawn while waiting
    if dry_run:
        print(f"Dry run: would merge PR {number}.")
    else:
        # Fails if the head changed while waiting.
        _gh(
            "pr", "merge", str(number), "--repo", repo, "--merge",
            "--match-head-commit", head_sha,
        )  # fmt: skip
        print(f"Merged PR {number}.")
    _remove_label(repo, label, number)


def merge_next(
    repo: str,
    label: str,
    project_slug: str,
    dry_run: bool,
    handled: Collection[int] = (),
) -> Optional[int]:
    """Merge the earliest labelled PR, or reject it with a comment and drop its label.

    Rejecting stops one stuck PR blocking the rest. If main's deploy failed, the
    run exits instead, as nothing can merge until it is fixed.

    Args:
        repo (str): `owner/name`.
        label (str): Ready-to-merge label.
        project_slug (str): CircleCI project slug, e.g. `gh/owner/name`.
        dry_run (bool): Run every check but don't merge.
        handled (Collection[int]): PRs already dealt with in this run.

    Returns:
        Optional[int]: The PR handled, or None if none are left.
    """
    pr = next_in_queue(_labelled_prs(repo, label), handled)
    if pr is None:
        return None
    number = pr["number"]

    try:
        _merge_pr(repo, label, number, project_slug, dry_run)
    except NotMergeable as error:
        _reject(repo, label, number, f"Not merged: {error}")
    except MainDeployFailed as error:
        reason = f"Not merged: {error} Re-add the label once it is fixed."
        _reject(repo, label, number, reason)
        sys.exit(f"{error} Fix it before anything else merges.")
    except ERRORS as error:
        reason = f"Not merged: the merge bot hit an error ({_describe(error)}). Re-add the label to retry."  # fmt: skip
        _reject(repo, label, number, reason)
    return number


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--label", default="ready-to-merge")
    parser.add_argument("--project-slug", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--announce",
        type=int,
        metavar="PR",
        help="Comment PR's queue position, then exit.",
    )
    args = parser.parse_args()

    if args.announce:
        announce_queued(args.repo, args.label, args.announce)
        return

    if not os.environ.get("CIRCLECI_TOKEN"):
        sys.exit("CIRCLECI_TOKEN is not set.")

    # Each merge starts a main deploy, which the next PR waits for.
    handled: set[int] = set()
    while (
        number := merge_next(
            args.repo, args.label, args.project_slug, args.dry_run, handled
        )
    ) is not None:
        handled.add(number)


if __name__ == "__main__":
    main()
