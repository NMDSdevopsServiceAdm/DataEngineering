"""
Merge `ready-to-merge` PRs one at a time, in the order they were labelled.

GitHub merges the code before CircleCI deploys it, so a PR merged while the
previous deploy is still running fails at prod terraform. Each PR is updated
from main, checked, and merged only once the deploy of main's tip has finished.

Needs `gh` authenticated (GH_TOKEN) and CIRCLECI_TOKEN in the environment.
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
# Newest pipelines to scan for a commit's workflows.
PIPELINES_TO_SCAN = 20
FAILED_STATUSES = {"failed", "failing", "error", "canceled", "unauthorized"}
FAILED_BUCKETS = {"fail", "cancel"}
# This script's own workflow job, ignored so it doesn't wait on itself.
OWN_CHECK_NAME = "merge-when-ready"
POLL_SECONDS = 60
# CircleCI should start a pipeline within minutes of a push or merge.
APPEARANCE_TIMEOUT_SECONDS = 600
# A PR's own CircleCI workflows should finish well inside this.
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
    """Classify CircleCI workflows from their statuses.

    Args:
        statuses (Optional[list[str]]): Workflow statuses, or None if no
            workflow was found. Unknown statuses count as pending.

    Returns:
        State: FAILED if any workflow failed or was cancelled, CLEAR if all
            succeeded, otherwise PENDING (running, awaiting approval or not
            yet started).
    """
    if not statuses:
        return State.PENDING
    if any(status in FAILED_STATUSES for status in statuses):
        return State.FAILED
    if all(status == "success" for status in statuses):
        return State.CLEAR
    return State.PENDING


def checks_state(checks: list[dict]) -> State:
    """Classify a PR's checks, ignoring this script's own workflow job.

    Args:
        checks (list[dict]): PR checks from `gh pr checks --json name,bucket`.

    Returns:
        State: FAILED if any check failed or was cancelled, PENDING if any is
            running or none have been reported yet (e.g. just after a branch
            update), otherwise CLEAR.
    """
    buckets = [c["bucket"] for c in checks if c["name"] != OWN_CHECK_NAME]
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
    """Return the workflow statuses of the newest pipeline run for a commit.

    Args:
        pipelines (list[dict]): CircleCI pipelines, newest first.
        sha (str): Commit the pipeline must have run for.
        get_workflows (Callable[[str], list[dict]]): Lists a pipeline's
            workflows (dicts with `name` and `status`) from its id.
        workflow_name (Optional[str]): Only count workflows with this name.

    Returns:
        Optional[list[str]]: Statuses from the first pipeline for `sha` that has
            a matching workflow, or None if there is none yet.
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
    """Return when `label` was last added, or an empty string if unknown."""
    return max(
        (
            e["created_at"]
            for e in events
            if e.get("event") == "labeled" and e.get("label", {}).get("name") == label
        ),
        default="",
    )


def next_in_queue(prs: list[dict], handled: Collection[int] = ()) -> Optional[dict]:
    """Return the PR labelled earliest, skipping `handled`, or None if none left.

    Args:
        prs (list[dict]): PRs with `number` and `labelled_at`.
        handled (Collection[int]): PR numbers already dealt with in this run.

    Returns:
        Optional[dict]: Earliest labelled PR (lowest number on a tie). PRs with
            an unknown label time go last.
    """
    return min(
        (pr for pr in prs if pr["number"] not in handled),
        key=lambda pr: (pr["labelled_at"] or "~", pr["number"]),
        default=None,
    )


def _describe(error: Exception) -> str:
    detail = getattr(error, "stderr", None) or str(error)
    return detail.strip()[:MAX_DETAIL_CHARS]


def _retry(call: Callable[[], T]) -> T:
    """Retry a read-only call so a brief outage doesn't reject a PR."""
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


def _wait_while_pending(
    description: str, get_state: Callable[[], State], timeout: Optional[int]
) -> Optional[State]:
    """Poll until `get_state` is no longer PENDING; None if `timeout` passes first."""
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
    """Wait for CircleCI workflows to appear, then to finish.

    Args:
        description (str): What is waited for, for the log.
        fetch (Callable[[], Optional[list[str]]]): Current workflow statuses,
            or None if none exist yet.
        finish_timeout (Optional[int]): Seconds to wait for them to finish,
            or None to wait as long as it takes (e.g. a manual approval).

    Returns:
        Optional[State]: CLEAR or FAILED, or None if no workflow appeared
            within the appearance timeout.

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
    """Bring one PR up to date, wait for its checks and main's deploy, merge it.

    Args:
        repo (str): `owner/name` of the GitHub repository.
        label (str): Label marking PRs ready to merge, removed once done.
        number (int): PR number.
        project_slug (str): CircleCI project slug, e.g. `gh/owner/name`.
        dry_run (bool): Run every check but do not merge.

    Raises:
        NotMergeable: If the PR's checks fail or main keeps moving past it.
        MainDeployFailed: If main's tip has no deploy or its deploy failed.
    """
    for _ in range(MAX_ROUNDS):
        head_sha, head_branch = _pr_head(repo, number)
        if _behind_by(repo, head_sha) > 0:
            print(f"PR {number} is behind main; updating its branch.")
            _gh("api", "-X", "PUT", f"repos/{repo}/pulls/{number}/update-branch")
            time.sleep(POLL_SECONDS)  # let CircleCI pick up the new head
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

        # The PR's checks can report before CircleCI has finished every workflow.
        circleci = _wait_for_workflows(
            f"CircleCI workflows for PR {number}",
            lambda: _statuses_for_commit(project_slug, head_branch, head_sha),
            PR_WORKFLOWS_TIMEOUT_SECONDS,
        )
        if circleci is None:
            raise NotMergeable("CircleCI never started checks on its latest commit.")
        if circleci is State.FAILED:
            raise NotMergeable("CircleCI workflows failed. Fix them and re-add the label.")  # fmt: skip

        # The deploy of main's tip, not just the latest one found: right after a
        # merge CircleCI may not have created that pipeline yet.
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

        if _behind_by(repo, head_sha) == 0:  # main can move during the wait
            break
    else:
        raise NotMergeable("Main kept moving past this PR. Re-add the label to retry.")

    if dry_run:
        print(f"Dry run: would merge PR {number}.")
    else:
        _gh("pr", "merge", str(number), "--repo", repo, "--merge")
        print(f"Merged PR {number}.")
    _remove_label(repo, label, number)


def merge_next(
    repo: str,
    label: str,
    project_slug: str,
    dry_run: bool,
    handled: Collection[int] = (),
) -> Optional[int]:
    """Merge the earliest labelled PR once it is current, green and main is deployed.

    A PR that can't be merged gets a comment saying why and loses its label, so
    one stuck PR never blocks the ones behind it. If main's deploy failed, the
    PR is commented on and the run exits, since nothing can merge until it is
    fixed.

    Args:
        repo (str): `owner/name` of the GitHub repository.
        label (str): Label marking PRs ready to merge.
        project_slug (str): CircleCI project slug, e.g. `gh/owner/name`.
        dry_run (bool): Run every check but do not merge.
        handled (Collection[int]): PR numbers already dealt with in this run.

    Returns:
        Optional[int]: Number of the PR handled (merged, dry-run passed or
            removed from the queue), or None if none are left.
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
    args = parser.parse_args()

    if not os.environ.get("CIRCLECI_TOKEN"):
        sys.exit("CIRCLECI_TOKEN is not set.")

    # Merging a PR starts a new main deploy, so the next one waits for it.
    handled: set[int] = set()
    while (
        number := merge_next(
            args.repo, args.label, args.project_slug, args.dry_run, handled
        )
    ) is not None:
        handled.add(number)


if __name__ == "__main__":
    main()
