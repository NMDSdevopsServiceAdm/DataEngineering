"""
Merge labelled PRs into main one at a time, as a stand-in for GitHub's merge
queue.

GitHub merges the code before CircleCI deploys it, so a second PR merged while
the first is still deploying fails at prod terraform and leaves main ahead of
what is deployed. For the oldest PR carrying the label, this script updates the
branch from main, waits for its checks, then waits for the latest main deploy
to finish before merging. It stops without merging if that deploy failed.

Needs `gh` authenticated (GH_TOKEN) and CIRCLECI_TOKEN in the environment.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from enum import Enum
from typing import Iterable, Optional

CIRCLECI_API = "https://circleci.com/api/v2"
MAIN_DEPLOY_WORKFLOW = "test-plan-approve-and-deploy-to-main"
# Newest pipelines on main to scan: some are delete-hook pipelines with no deploy.
PIPELINES_TO_SCAN = 10
FAILED_STATUSES = {"failed", "error", "canceled", "unauthorized"}
POLL_SECONDS = 60
MAX_BRANCH_UPDATES = 5
# This script's own workflow job, ignored so it doesn't wait on itself.
OWN_CHECK_NAME = "merge-when-ready"
FAILED_BUCKETS = {"fail", "cancel"}


class MainDeployState(str, Enum):
    CLEAR = "clear"
    PENDING = "pending"
    FAILED = "failed"


class ChecksState(str, Enum):
    PASSED = "passed"
    PENDING = "pending"
    FAILED = "failed"


def checks_state(checks: list[dict]) -> ChecksState:
    """Classify a PR's checks, ignoring this script's own workflow job.

    Args:
        checks (list[dict]): PR checks from `gh pr checks --json name,bucket`.

    Returns:
        ChecksState: FAILED if any check failed or was cancelled, PENDING if
            any is running or none have been reported yet (e.g. just after a
            branch update), otherwise PASSED.
    """
    buckets = [c["bucket"] for c in checks if c["name"] != OWN_CHECK_NAME]
    if any(bucket in FAILED_BUCKETS for bucket in buckets):
        return ChecksState.FAILED
    if not buckets or "pending" in buckets:
        return ChecksState.PENDING
    return ChecksState.PASSED


def main_deploy_state(workflow_statuses: Optional[list[str]]) -> MainDeployState:
    """Classify the latest main deploy from its CircleCI workflow statuses.

    Args:
        workflow_statuses (Optional[list[str]]): Statuses of the latest main
            deploy workflow, or None if none was found. Unknown statuses count
            as pending.

    Returns:
        MainDeployState: FAILED if any workflow failed or was cancelled, CLEAR
            if all succeeded, otherwise PENDING (running, awaiting approval or
            not yet started).
    """
    if not workflow_statuses:
        return MainDeployState.PENDING
    if any(status in FAILED_STATUSES for status in workflow_statuses):
        return MainDeployState.FAILED
    if all(status == "success" for status in workflow_statuses):
        return MainDeployState.CLEAR
    return MainDeployState.PENDING


def first_deploy_workflow_statuses(
    pipelines_workflows: Iterable[list[dict]],
) -> Optional[list[str]]:
    """Return the statuses of the newest main deploy workflow.

    Args:
        pipelines_workflows (Iterable[list[dict]]): Workflows (dicts with `name`
            and `status`) of each main pipeline, newest pipeline first.

    Returns:
        Optional[list[str]]: Statuses of the deploy workflows in the newest
            pipeline that has one, or None if no pipeline has one.
    """
    for workflows in pipelines_workflows:
        statuses = [w["status"] for w in workflows if w["name"] == MAIN_DEPLOY_WORKFLOW]
        if statuses:
            return statuses
    return None


def oldest_labelled_pr(prs: list[dict]) -> Optional[dict]:
    """Return the labelled PR with the lowest number, or None if there are none."""
    return min(prs, key=lambda pr: pr["number"], default=None)


def _circleci_get(path: str) -> dict:
    request = urllib.request.Request(
        f"{CIRCLECI_API}{path}", headers={"Circle-Token": os.environ["CIRCLECI_TOKEN"]}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def latest_main_deploy_statuses(project_slug: str) -> Optional[list[str]]:
    """Read the newest main deploy's workflow statuses from CircleCI."""
    pipelines = _circleci_get(f"/project/{project_slug}/pipeline?branch=main")["items"]
    return first_deploy_workflow_statuses(
        _circleci_get(f"/pipeline/{pipeline['id']}/workflow")["items"]
        for pipeline in pipelines[:PIPELINES_TO_SCAN]
    )


def _gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=check)


def _labelled_prs(repo: str, label: str) -> list[dict]:
    out = _gh(
        "pr", "list", "--repo", repo, "--label", label, "--state", "open",
        "--json", "number,headRefOid",
    ).stdout  # fmt: skip
    return json.loads(out)


def _pr_checks(repo: str, number: int) -> list[dict]:
    # gh exits non-zero while checks are pending or none exist: read stdout only.
    out = _gh(
        "pr", "checks", str(number), "--repo", repo, "--json", "name,bucket",
        check=False,
    ).stdout  # fmt: skip
    return json.loads(out) if out.strip() else []


def _behind_by(repo: str, head_sha: str) -> int:
    out = _gh("api", f"repos/{repo}/compare/main...{head_sha}", "--jq", ".behind_by")
    return int(out.stdout)


def _reject(repo: str, label: str, pr_number: int, reason: str) -> None:
    _gh("pr", "edit", str(pr_number), "--repo", repo, "--remove-label", label)
    _gh("pr", "comment", str(pr_number), "--repo", repo, "--body", reason)
    print(f"PR {pr_number} left the queue: {reason}")


def merge_next(repo: str, label: str, project_slug: str, dry_run: bool) -> bool:
    """Merge the oldest labelled PR once it is current, green and main is idle.

    Exits without merging if the latest main deploy failed, so nothing more
    merges until it is fixed.

    Args:
        repo (str): `owner/name` of the GitHub repository.
        label (str): Label marking PRs ready to merge.
        project_slug (str): CircleCI project slug, e.g. `gh/owner/name`.
        dry_run (bool): Run every check but do not merge.

    Returns:
        bool: True if a PR was handled (merged, dry-run passed or removed from
            the queue), False if none were labelled.
    """
    pr = oldest_labelled_pr(_labelled_prs(repo, label))
    if pr is None:
        return False
    number = pr["number"]

    for _ in range(MAX_BRANCH_UPDATES):
        head_sha = json.loads(
            _gh(
                "pr", "view", str(number), "--repo", repo, "--json", "headRefOid"
            ).stdout
        )["headRefOid"]
        if _behind_by(repo, head_sha) > 0:
            _gh("api", "-X", "PUT", f"repos/{repo}/pulls/{number}/update-branch")
            time.sleep(POLL_SECONDS)  # let CircleCI pick up the new head
            continue
        while (checks := checks_state(_pr_checks(repo, number))) is ChecksState.PENDING:
            print("PR checks still running; waiting.")
            time.sleep(POLL_SECONDS)
        if checks is ChecksState.FAILED:
            reason = "Checks failed, so it was removed from the merge queue."
            _reject(repo, label, number, reason)
            return True
        if _behind_by(repo, head_sha) == 0:
            break
    else:
        _reject(repo, label, number, "Main kept moving; re-add the label to retry.")
        return True

    while (
        state := main_deploy_state(latest_main_deploy_statuses(project_slug))
    ) is MainDeployState.PENDING:
        print("Main deploy still running or awaiting approval; waiting.")  # fmt: skip
        time.sleep(POLL_SECONDS)
    if state is MainDeployState.FAILED:
        sys.exit("Latest main deploy failed: fix it before anything else merges.")

    if dry_run:
        _gh("pr", "edit", str(number), "--repo", repo, "--remove-label", label)
        print(f"Dry run: PR {number} would be merged now.")
        return True
    _gh("pr", "merge", str(number), "--repo", repo, "--merge")
    _gh("pr", "edit", str(number), "--repo", repo, "--remove-label", label)
    print(f"Merged PR {number}.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True)
    parser.add_argument("--label", default="ready-to-merge")
    parser.add_argument("--project-slug", required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Merging a PR starts a new main deploy, so the next one waits for it.
    while merge_next(args.repo, args.label, args.project_slug, args.dry_run):
        if args.dry_run:
            break


if __name__ == "__main__":
    main()
