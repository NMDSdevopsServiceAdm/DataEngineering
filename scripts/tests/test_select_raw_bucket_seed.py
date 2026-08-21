import pytest

import scripts.select_raw_bucket_seed as job

trigger_path_cases = [
    pytest.param(
        "projects/_01_ingest/ascwds/jobs/ingest_ascwds_worker.py",
        id="returns_true_when_changed_path_is_under_ascwds_ingest_dir",
    ),
    pytest.param(
        "projects/_01_ingest/capacity_tracker/fargate/ingest.py",
        id="returns_true_when_changed_path_is_under_capacity_tracker_ingest_dir",
    ),
    pytest.param(
        "projects/_01_ingest/cqc_pir/jobs/ingest_cqc_pir.py",
        id="returns_true_when_changed_path_is_under_cqc_pir_ingest_dir",
    ),
    pytest.param(
        "projects/_01_ingest/ons_pd/jobs/ingest_ons_pd.py",
        id="returns_true_when_changed_path_is_under_ons_pd_ingest_dir",
    ),
    pytest.param(
        "terraform/pipeline/eventbridge.tf",
        id="returns_true_when_eventbridge_terraform_changed",
    ),
]


class TestShouldSeedRawBucket:
    @pytest.mark.parametrize("changed_path", trigger_path_cases)
    def test_returns_true_when_a_trigger_path_changed(self, changed_path: str):
        assert job.should_seed_raw_bucket([changed_path]) is True

    def test_returns_false_when_only_unrelated_paths_changed(self):
        changed_paths = ["README.md", "projects/_07_workforce_characteristics/foo.py"]

        assert job.should_seed_raw_bucket(changed_paths) is False

    def test_returns_false_when_cqc_api_ingest_dir_changed(self):
        # cqc_api is a sibling ingest domain that never reads the raw bucket --
        # a prefix match here would wrongly widen the trigger set.
        changed_paths = ["projects/_01_ingest/cqc_api/jobs/ingest_cqc_api.py"]

        assert job.should_seed_raw_bucket(changed_paths) is False

    def test_returns_false_for_empty_changed_paths(self):
        assert job.should_seed_raw_bucket([]) is False

    def test_returns_false_when_raw_bucket_iam_terraform_changed(self):
        assert job.should_seed_raw_bucket(["terraform/pipeline/iam.tf"]) is False

    def test_returns_false_when_raw_bucket_s3_terraform_changed(self):
        assert job.should_seed_raw_bucket(["terraform/pipeline/s3.tf"]) is False

    def test_returns_false_when_fargate_task_raw_bucket_wiring_changed(self):
        changed_paths = ["terraform/modules/fargate-task/iam.tf"]

        assert job.should_seed_raw_bucket(changed_paths) is False

    def test_returns_false_when_circleci_config_changed(self):
        assert job.should_seed_raw_bucket([".circleci/config.yml"]) is False


class TestMain:
    def test_prints_true_when_dry_run_changed_path_matches_a_trigger(self, capsys):
        job.main(["--changed-path", "projects/_01_ingest/ascwds/jobs/ingest.py"])

        assert capsys.readouterr().out.strip() == "true"

    def test_prints_false_when_dry_run_changed_path_does_not_match_any_trigger(
        self, capsys
    ):
        job.main(["--changed-path", "CHANGELOG.md"])

        assert capsys.readouterr().out.strip() == "false"
