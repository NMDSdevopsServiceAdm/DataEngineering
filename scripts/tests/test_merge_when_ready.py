import subprocess
from dataclasses import dataclass
from unittest.mock import Mock, patch

import pytest

import scripts.merge_when_ready as job

DEPLOY = job.MAIN_DEPLOY_WORKFLOW


@dataclass
class DeployStateTestCase:
    id: str
    statuses: list[str] | None
    expected: job.MainDeployState

    def as_pytest_param(self):
        return pytest.param(self.statuses, self.expected, id=self.id)


deploy_state_cases = [
    DeployStateTestCase("clear_when_all_succeeded", ["success"], job.MainDeployState.CLEAR),
    DeployStateTestCase("pending_when_awaiting_approval", ["on_hold"], job.MainDeployState.PENDING),
    DeployStateTestCase("pending_when_running", ["running"], job.MainDeployState.PENDING),
    DeployStateTestCase("pending_when_no_deploy_found", None, job.MainDeployState.PENDING),
    DeployStateTestCase("pending_when_status_unknown", ["something_new"], job.MainDeployState.PENDING),
    DeployStateTestCase("failed_when_any_failed", ["success", "failed"], job.MainDeployState.FAILED),
    DeployStateTestCase("failed_when_cancelled", ["canceled"], job.MainDeployState.FAILED),
]  # fmt: skip


class TestMainDeployState:
    @pytest.mark.parametrize(
        "statuses, expected", [c.as_pytest_param() for c in deploy_state_cases]
    )
    def test_returns_expected_state(self, statuses, expected):
        assert job.main_deploy_state(statuses) is expected


OWN = job.OWN_CHECK_NAME


@dataclass
class ChecksStateTestCase:
    id: str
    checks: list[dict]
    expected: job.ChecksState

    def as_pytest_param(self):
        return pytest.param(self.checks, self.expected, id=self.id)


checks_state_cases = [
    ChecksStateTestCase("passed_when_all_pass", [{"name": "test", "bucket": "pass"}], job.ChecksState.PASSED),
    ChecksStateTestCase("passed_when_skipped_checks_present", [{"name": "test", "bucket": "pass"}, {"name": "x", "bucket": "skipping"}], job.ChecksState.PASSED),
    ChecksStateTestCase("pending_when_any_running", [{"name": "test", "bucket": "pass"}, {"name": "lint", "bucket": "pending"}], job.ChecksState.PENDING),
    ChecksStateTestCase("pending_when_no_checks_yet", [], job.ChecksState.PENDING),
    ChecksStateTestCase("failed_when_any_failed", [{"name": "test", "bucket": "fail"}, {"name": "lint", "bucket": "pending"}], job.ChecksState.FAILED),
    ChecksStateTestCase("failed_when_cancelled", [{"name": "test", "bucket": "cancel"}], job.ChecksState.FAILED),
    ChecksStateTestCase("ignores_own_pending_check", [{"name": "test", "bucket": "pass"}, {"name": OWN, "bucket": "pending"}], job.ChecksState.PASSED),
    ChecksStateTestCase("pending_when_only_own_check", [{"name": OWN, "bucket": "pending"}], job.ChecksState.PENDING),
]  # fmt: skip


class TestChecksState:
    @pytest.mark.parametrize(
        "checks, expected", [c.as_pytest_param() for c in checks_state_cases]
    )
    def test_returns_expected_state(self, checks, expected):
        assert job.checks_state(checks) is expected


class TestFirstDeployWorkflowStatuses:
    def test_skips_pipelines_without_deploy_workflow(self):
        pipelines = [
            [{"name": "delete-dev-env", "status": "success"}],
            [{"name": DEPLOY, "status": "on_hold"}],
        ]

        assert job.first_deploy_workflow_statuses(pipelines) == ["on_hold"]

    def test_uses_newest_pipeline_with_deploy_workflow(self):
        pipelines = [
            [{"name": DEPLOY, "status": "running"}],
            [{"name": DEPLOY, "status": "success"}],
        ]

        assert job.first_deploy_workflow_statuses(pipelines) == ["running"]

    def test_ignores_other_workflows_in_the_pipeline(self):
        pipelines = [
            [
                {"name": "merge-queue-checks", "status": "failed"},
                {"name": DEPLOY, "status": "success"},
            ]
        ]

        assert job.first_deploy_workflow_statuses(pipelines) == ["success"]

    def test_returns_none_when_no_pipeline_has_deploy_workflow(self):
        pipelines = [[{"name": "delete-dev-env", "status": "success"}]]

        assert job.first_deploy_workflow_statuses(pipelines) is None


class TestOldestLabelledPr:
    def test_returns_lowest_pr_number(self):
        prs = [{"number": 1700}, {"number": 1650}, {"number": 1680}]

        assert job.oldest_labelled_pr(prs) == {"number": 1650}

    def test_returns_none_when_no_prs(self):
        assert job.oldest_labelled_pr([]) is None


PATCH_PATH = "scripts.merge_when_ready"
REPO = "owner/name"
LABEL = "ready-to-merge"


@patch(f"{PATCH_PATH}._reject")
@patch(f"{PATCH_PATH}._merge_pr")
@patch(f"{PATCH_PATH}._labelled_prs")
class TestMergeNext:
    def test_returns_false_when_no_labelled_prs(
        self, labelled_prs_mock: Mock, merge_pr_mock: Mock, reject_mock: Mock
    ):
        labelled_prs_mock.return_value = []

        assert job.merge_next(REPO, LABEL, "gh/owner/name", False) is False
        merge_pr_mock.assert_not_called()

    def test_merges_oldest_labelled_pr(
        self, labelled_prs_mock: Mock, merge_pr_mock: Mock, reject_mock: Mock
    ):
        labelled_prs_mock.return_value = [{"number": 9}, {"number": 4}]

        assert job.merge_next(REPO, LABEL, "gh/owner/name", False) is True
        merge_pr_mock.assert_called_once_with(REPO, LABEL, 4, "gh/owner/name", False)
        reject_mock.assert_not_called()

    def test_rejects_pr_with_reason_and_carries_on_when_not_mergeable(
        self, labelled_prs_mock: Mock, merge_pr_mock: Mock, reject_mock: Mock
    ):
        labelled_prs_mock.return_value = [{"number": 4}]
        merge_pr_mock.side_effect = job.NotMergeable("Checks failed.")

        assert job.merge_next(REPO, LABEL, "gh/owner/name", False) is True
        assert "Checks failed." in reject_mock.call_args.args[3]

    def test_rejects_pr_with_error_detail_when_a_command_fails(
        self, labelled_prs_mock: Mock, merge_pr_mock: Mock, reject_mock: Mock
    ):
        labelled_prs_mock.return_value = [{"number": 4}]
        merge_pr_mock.side_effect = subprocess.CalledProcessError(
            1, "gh", stderr="merge conflict\n"
        )

        assert job.merge_next(REPO, LABEL, "gh/owner/name", False) is True
        assert "merge conflict" in reject_mock.call_args.args[3]

    def test_comments_and_exits_when_main_deploy_failed(
        self, labelled_prs_mock: Mock, merge_pr_mock: Mock, reject_mock: Mock
    ):
        labelled_prs_mock.return_value = [{"number": 4}]
        merge_pr_mock.side_effect = job.MainDeployFailed()

        with pytest.raises(SystemExit):
            job.merge_next(REPO, LABEL, "gh/owner/name", False)

        assert "main deploy failed" in reject_mock.call_args.args[3]
