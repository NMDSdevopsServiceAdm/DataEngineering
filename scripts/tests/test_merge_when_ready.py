import subprocess
from dataclasses import dataclass
from typing import Any
from unittest.mock import DEFAULT, patch

import pytest

import scripts.merge_when_ready as job

PATCH_PATH = "scripts.merge_when_ready"
REPO = "owner/name"
LABEL = "ready-to-merge"
SLUG = "gh/owner/name"
DEPLOY = job.MAIN_DEPLOY_WORKFLOW
OWN = job.OWN_CHECK_NAME
PASS = {"name": "test", "bucket": "pass"}
MERGE_CALL = (
    "pr",
    "merge",
    "7",
    "--repo",
    REPO,
    "--merge",
    "--match-head-commit",
    "headsha",
)


@dataclass
class StateTestCase:
    id: str
    data: Any
    expected: job.State

    def as_pytest_param(self):
        return pytest.param(self.data, self.expected, id=self.id)


workflow_cases = [
    StateTestCase("clear_when_all_succeeded", ["success"], job.State.CLEAR),
    StateTestCase("pending_when_awaiting_approval", ["on_hold"], job.State.PENDING),
    StateTestCase("pending_when_running", ["running"], job.State.PENDING),
    StateTestCase("pending_when_none_found", None, job.State.PENDING),
    StateTestCase("pending_when_status_unknown", ["something_new"], job.State.PENDING),
    StateTestCase("failed_when_any_failed", ["success", "failed"], job.State.FAILED),
    StateTestCase("failed_when_cancelled", ["canceled"], job.State.FAILED),
    StateTestCase("failed_when_failing", ["failing"], job.State.FAILED),
]

checks_cases = [
    StateTestCase("clear_when_all_pass", [PASS], job.State.CLEAR),
    StateTestCase("clear_when_some_skipped", [PASS, {"name": "x", "bucket": "skipping"}], job.State.CLEAR),
    StateTestCase("pending_when_any_running", [PASS, {"name": "lint", "bucket": "pending"}], job.State.PENDING),
    StateTestCase("pending_when_no_checks_yet", [], job.State.PENDING),
    StateTestCase("failed_when_any_failed", [{"name": "test", "bucket": "fail"}, {"name": "lint", "bucket": "pending"}], job.State.FAILED),
    StateTestCase("failed_when_cancelled", [{"name": "test", "bucket": "cancel"}], job.State.FAILED),
    StateTestCase("ignores_own_pending_check", [PASS, {"name": OWN, "bucket": "pending"}], job.State.CLEAR),
    StateTestCase("pending_when_only_own_check", [{"name": OWN, "bucket": "pending"}], job.State.PENDING),
]  # fmt: skip


class TestWorkflowsState:
    @pytest.mark.parametrize(
        "statuses, expected", [c.as_pytest_param() for c in workflow_cases]
    )
    def test_returns_expected_state(self, statuses, expected):
        assert job.workflows_state(statuses) is expected


class TestChecksState:
    @pytest.mark.parametrize(
        "checks, expected", [c.as_pytest_param() for c in checks_cases]
    )
    def test_returns_expected_state(self, checks, expected):
        assert job.checks_state(checks) is expected


def pipeline(pipeline_id: str, sha: str) -> dict:
    return {"id": pipeline_id, "vcs": {"revision": sha}}


class TestCommitWorkflowStatuses:
    def test_returns_statuses_of_pipeline_for_the_commit(self):
        workflows = {"p1": [{"name": DEPLOY, "status": "running"}]}

        statuses = job.commit_workflow_statuses(
            [pipeline("p1", "tip")], "tip", lambda pid: workflows[pid]
        )

        assert statuses == ["running"]

    def test_returns_none_when_no_pipeline_ran_for_the_commit(self):
        # The previous deploy must not stand in for a merge not yet deployed.
        pipelines = [pipeline("p1", "older")]

        statuses = job.commit_workflow_statuses(
            pipelines, "tip", lambda pid: [{"name": DEPLOY, "status": "success"}]
        )

        assert statuses is None

    def test_ignores_workflows_with_other_names(self):
        workflows = [
            {"name": "delete-dev-env", "status": "failed"},
            {"name": DEPLOY, "status": "success"},
        ]

        statuses = job.commit_workflow_statuses(
            [pipeline("p1", "tip")], "tip", lambda pid: workflows, DEPLOY
        )

        assert statuses == ["success"]

    def test_skips_pipelines_for_the_commit_without_a_matching_workflow(self):
        workflows = {
            "p1": [{"name": "delete-dev-env", "status": "success"}],
            "p2": [{"name": DEPLOY, "status": "on_hold"}],
        }

        statuses = job.commit_workflow_statuses(
            [pipeline("p1", "tip"), pipeline("p2", "tip")],
            "tip",
            lambda pid: workflows[pid],
            DEPLOY,
        )

        assert statuses == ["on_hold"]

    def test_counts_every_workflow_when_no_name_given(self):
        workflows = [
            {"name": "a", "status": "success"},
            {"name": "b", "status": "running"},
        ]

        statuses = job.commit_workflow_statuses(
            [pipeline("p1", "sha")], "sha", lambda pid: workflows
        )

        assert statuses == ["success", "running"]


class TestLabelledAt:
    def test_returns_latest_time_the_label_was_added(self):
        events = [
            {"event": "labeled", "label": {"name": LABEL}, "created_at": "2026-10-01"},
            {"event": "labeled", "label": {"name": LABEL}, "created_at": "2026-10-05"},
        ]

        assert job.labelled_at(events, LABEL) == "2026-10-05"

    def test_ignores_other_labels_and_events(self):
        events = [
            {
                "event": "labeled",
                "label": {"name": "other"},
                "created_at": "2026-10-09",
            },
            {
                "event": "unlabeled",
                "label": {"name": LABEL},
                "created_at": "2026-10-08",
            },
            {"event": "labeled", "label": {"name": LABEL}, "created_at": "2026-10-02"},
        ]

        assert job.labelled_at(events, LABEL) == "2026-10-02"

    def test_returns_empty_string_when_never_labelled(self):
        assert job.labelled_at([], LABEL) == ""


class TestNextInQueue:
    def test_returns_pr_labelled_first_not_lowest_number(self):
        prs = [
            {"number": 1650, "labelled_at": "2026-10-05"},
            {"number": 1700, "labelled_at": "2026-10-01"},
        ]

        assert job.next_in_queue(prs)["number"] == 1700

    def test_returns_lowest_number_on_a_tie(self):
        prs = [
            {"number": 9, "labelled_at": "2026-10-01"},
            {"number": 4, "labelled_at": "2026-10-01"},
        ]

        assert job.next_in_queue(prs)["number"] == 4

    def test_puts_unknown_label_time_last(self):
        prs = [
            {"number": 1, "labelled_at": ""},
            {"number": 2, "labelled_at": "2026-10-01"},
        ]

        assert job.next_in_queue(prs)["number"] == 2

    def test_skips_handled_prs(self):
        prs = [
            {"number": 1, "labelled_at": "2026-10-01"},
            {"number": 2, "labelled_at": "2026-10-02"},
        ]

        assert job.next_in_queue(prs, handled={1})["number"] == 2

    def test_returns_none_when_no_prs_left(self):
        assert job.next_in_queue([{"number": 1, "labelled_at": ""}], {1}) is None


def review(login: str, state: str, submitted_at: str) -> dict:
    return {"author": {"login": login}, "state": state, "submittedAt": submitted_at}


@dataclass
class ApprovalTestCase:
    id: str
    reviews: list[dict]
    expected: bool

    def as_pytest_param(self):
        return pytest.param(self.reviews, self.expected, id=self.id)


approval_cases = [
    ApprovalTestCase("approved_with_one_approval", [review("a", "APPROVED", "1")], True),
    ApprovalTestCase("not_approved_with_no_reviews", [], False),
    ApprovalTestCase("not_approved_with_only_comments", [review("a", "COMMENTED", "1")], False),
    ApprovalTestCase("approval_survives_a_later_comment", [review("a", "APPROVED", "1"), review("a", "COMMENTED", "2")], True),
    ApprovalTestCase("not_approved_when_changes_requested_after_approval", [review("a", "APPROVED", "1"), review("a", "CHANGES_REQUESTED", "2")], False),
    ApprovalTestCase("approved_when_reviewer_re_approves_after_changes", [review("a", "CHANGES_REQUESTED", "1"), review("a", "APPROVED", "2")], True),
    ApprovalTestCase("not_approved_when_approval_dismissed", [review("a", "APPROVED", "1"), review("a", "DISMISSED", "2")], False),
    ApprovalTestCase("not_approved_when_another_reviewer_requests_changes", [review("a", "APPROVED", "1"), review("b", "CHANGES_REQUESTED", "2")], False),
    ApprovalTestCase("uses_submission_time_not_list_order", [review("a", "APPROVED", "2"), review("a", "CHANGES_REQUESTED", "1")], True),
]  # fmt: skip


class TestIsApproved:
    @pytest.mark.parametrize(
        "reviews, expected", [c.as_pytest_param() for c in approval_cases]
    )
    def test_returns_expected_result(self, reviews, expected):
        assert job.is_approved(reviews) is expected


class TestRequireApproval:
    @patch(f"{PATCH_PATH}._gh_json")
    def test_passes_when_approved(self, gh_json_mock):
        gh_json_mock.return_value = {"reviews": [review("a", "APPROVED", "1")]}

        job._require_approval(REPO, 7)

    @patch(f"{PATCH_PATH}._gh_json")
    def test_raises_not_mergeable_when_not_approved(self, gh_json_mock):
        gh_json_mock.return_value = {"reviews": []}

        with pytest.raises(job.NotMergeable, match="approving review"):
            job._require_approval(REPO, 7)


@pytest.fixture
def stubs():
    """Patch every outside call so `_merge_pr` runs with an all-clear world."""
    with (
        patch.multiple(
            PATCH_PATH,
            _pr_head=DEFAULT,
            _behind_by=DEFAULT,
            _gh=DEFAULT,
            _pr_checks=DEFAULT,
            _statuses_for_commit=DEFAULT,
            _main_tip=DEFAULT,
            _require_approval=DEFAULT,
        ) as mocks,
        patch(f"{PATCH_PATH}.time.sleep"),
    ):
        mocks["_pr_head"].return_value = ("headsha", "feature-branch")
        mocks["_behind_by"].return_value = 0
        mocks["_pr_checks"].return_value = [PASS]
        mocks["_statuses_for_commit"].return_value = ["success"]
        mocks["_main_tip"].return_value = "tipsha123"
        yield mocks


def gh_calls(stubs) -> list[tuple]:
    return [call.args for call in stubs["_gh"].call_args_list]


def merge_pr():
    job._merge_pr(REPO, LABEL, 7, SLUG, dry_run=False)


class TestMergePr:
    def test_merges_and_removes_label_when_current_green_and_main_deployed(self, stubs):
        merge_pr()

        calls = gh_calls(stubs)
        assert MERGE_CALL in calls
        assert ("pr", "edit", "7", "--repo", REPO, "--remove-label", LABEL) in calls

    def test_dry_run_does_not_merge_but_removes_label(self, stubs):
        job._merge_pr(REPO, LABEL, 7, SLUG, dry_run=True)

        calls = gh_calls(stubs)
        assert not any(c[:2] == ("pr", "merge") for c in calls)
        assert ("pr", "edit", "7", "--repo", REPO, "--remove-label", LABEL) in calls

    def test_updates_branch_when_behind_main_then_merges(self, stubs):
        # Behind at the start, then up to date before and after the deploy wait.
        stubs["_behind_by"].side_effect = [3, 0, 0]

        merge_pr()

        calls = gh_calls(stubs)
        assert ("api", "-X", "PUT", f"repos/{REPO}/pulls/7/update-branch") in calls
        assert MERGE_CALL in calls

    def test_updates_branch_again_when_main_moves_during_deploy_wait(self, stubs):
        stubs["_behind_by"].side_effect = [0, 2, 2, 0, 0]

        merge_pr()

        updates = [c for c in gh_calls(stubs) if c[:3] == ("api", "-X", "PUT")]
        assert len(updates) == 1
        assert MERGE_CALL in gh_calls(stubs)

    def test_raises_not_mergeable_when_main_keeps_moving(self, stubs):
        stubs["_behind_by"].return_value = 2

        with pytest.raises(job.NotMergeable, match="Main kept moving"):
            merge_pr()

        assert not any(c[:2] == ("pr", "merge") for c in gh_calls(stubs))

    def test_raises_not_mergeable_when_pr_checks_fail(self, stubs):
        stubs["_pr_checks"].return_value = [{"name": "test", "bucket": "fail"}]

        with pytest.raises(job.NotMergeable, match="Checks failed"):
            merge_pr()

    def test_raises_not_mergeable_when_circleci_workflows_fail(self, stubs):
        def statuses(slug, branch, sha, workflow_name=None):
            return ["failed"] if branch == "feature-branch" else ["success"]

        stubs["_statuses_for_commit"].side_effect = statuses

        with pytest.raises(job.NotMergeable, match="CircleCI workflows failed"):
            merge_pr()

    def test_raises_not_mergeable_when_circleci_never_starts_on_the_pr(self, stubs):
        stubs["_statuses_for_commit"].return_value = None

        with pytest.raises(job.NotMergeable, match="never started"):
            merge_pr()

    def test_raises_main_deploy_failed_when_tip_deploy_failed(self, stubs):
        def statuses(slug, branch, sha, workflow_name=None):
            return ["failed"] if branch == "main" else ["success"]

        stubs["_statuses_for_commit"].side_effect = statuses

        with pytest.raises(job.MainDeployFailed, match="tipsha12 failed"):
            merge_pr()

    def test_raises_main_deploy_failed_when_no_deploy_appears_for_tip(self, stubs):
        def statuses(slug, branch, sha, workflow_name=None):
            return None if branch == "main" else ["success"]

        stubs["_statuses_for_commit"].side_effect = statuses

        with pytest.raises(job.MainDeployFailed, match="No main deploy found"):
            merge_pr()

        assert not any(c[:2] == ("pr", "merge") for c in gh_calls(stubs))

    def test_waits_for_main_deploy_awaiting_approval_before_merging(self, stubs):
        # Stands in for the approval arriving after a few polls.
        main_polls = iter(
            [["on_hold"], ["on_hold"], ["on_hold"], ["on_hold"], ["success"]]
        )

        def deploy_then_approved(slug, branch, sha, workflow_name=None):
            return next(main_polls) if branch == "main" else ["success"]

        stubs["_statuses_for_commit"].side_effect = deploy_then_approved

        merge_pr()

        assert MERGE_CALL in gh_calls(stubs)

    def test_raises_not_mergeable_without_waiting_when_not_approved(self, stubs):
        stubs["_require_approval"].side_effect = job.NotMergeable("needs review")

        with pytest.raises(job.NotMergeable, match="needs review"):
            merge_pr()

        stubs["_pr_checks"].assert_not_called()

    def test_does_not_merge_when_approval_is_withdrawn_during_the_waits(self, stubs):
        stubs["_require_approval"].side_effect = [
            None,
            job.NotMergeable("needs review"),
        ]

        with pytest.raises(job.NotMergeable, match="needs review"):
            merge_pr()

        assert not any(c[:2] == ("pr", "merge") for c in gh_calls(stubs))

    def test_does_not_raise_when_label_removal_fails_after_merge(self, stubs):
        def gh(*args, check=True):
            if args[:2] == ("pr", "edit"):
                raise subprocess.CalledProcessError(1, "gh", stderr="boom")

        stubs["_gh"].side_effect = gh

        merge_pr()


@patch(f"{PATCH_PATH}._reject")
@patch(f"{PATCH_PATH}._merge_pr")
@patch(f"{PATCH_PATH}._labelled_prs")
class TestMergeNext:
    def test_returns_none_when_no_labelled_prs(
        self, labelled_prs_mock, merge_pr_mock, reject_mock
    ):
        labelled_prs_mock.return_value = []

        assert job.merge_next(REPO, LABEL, SLUG, False) is None
        merge_pr_mock.assert_not_called()

    def test_merges_pr_labelled_first(
        self, labelled_prs_mock, merge_pr_mock, reject_mock
    ):
        labelled_prs_mock.return_value = [
            {"number": 4, "labelled_at": "2026-10-05"},
            {"number": 9, "labelled_at": "2026-10-01"},
        ]

        assert job.merge_next(REPO, LABEL, SLUG, False) == 9
        merge_pr_mock.assert_called_once_with(REPO, LABEL, 9, SLUG, False)
        reject_mock.assert_not_called()

    def test_skips_prs_already_handled_in_this_run(
        self, labelled_prs_mock, merge_pr_mock, reject_mock
    ):
        labelled_prs_mock.return_value = [{"number": 4, "labelled_at": ""}]

        assert job.merge_next(REPO, LABEL, SLUG, False, handled={4}) is None

    def test_rejects_pr_with_reason_and_carries_on_when_not_mergeable(
        self, labelled_prs_mock, merge_pr_mock, reject_mock
    ):
        labelled_prs_mock.return_value = [{"number": 4, "labelled_at": ""}]
        merge_pr_mock.side_effect = job.NotMergeable("Checks failed.")

        assert job.merge_next(REPO, LABEL, SLUG, False) == 4
        assert "Checks failed." in reject_mock.call_args.args[3]

    @pytest.mark.parametrize(
        "error",
        [
            subprocess.CalledProcessError(1, "gh", stderr="merge conflict\n"),
            TimeoutError("merge conflict"),
        ],
        ids=["command_failed", "timed_out"],
    )
    def test_rejects_pr_with_error_detail_when_a_call_fails(
        self, labelled_prs_mock, merge_pr_mock, reject_mock, error
    ):
        labelled_prs_mock.return_value = [{"number": 4, "labelled_at": ""}]
        merge_pr_mock.side_effect = error

        assert job.merge_next(REPO, LABEL, SLUG, False) == 4
        assert "merge conflict" in reject_mock.call_args.args[3]

    def test_comments_and_exits_when_main_deploy_failed(
        self, labelled_prs_mock, merge_pr_mock, reject_mock
    ):
        labelled_prs_mock.return_value = [{"number": 4, "labelled_at": ""}]
        merge_pr_mock.side_effect = job.MainDeployFailed("The main deploy failed.")

        with pytest.raises(SystemExit):
            job.merge_next(REPO, LABEL, SLUG, False)

        assert "main deploy failed" in reject_mock.call_args.args[3]
