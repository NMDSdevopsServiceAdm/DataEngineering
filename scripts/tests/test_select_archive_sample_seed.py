import pytest

import scripts.select_archive_sample_seed as job

trigger_path_cases = [
    pytest.param(
        "projects/_03_independent_cqc/_09_archive_estimates/fargate/archive_job_role_estimates.py",
        id="returns_true_when_changed_path_is_under_archive_estimates_dir",
    ),
    pytest.param(
        "projects/_03_independent_cqc/_09_archive_estimates/fargate/utils/archive_utils.py",
        id="returns_true_when_changed_path_is_under_archive_estimates_utils_dir",
    ),
    pytest.param(
        "projects/_08_publication/_01_job_role_estimates/fargate/_01_merge_pub_data.py",
        id="returns_true_when_changed_path_is_under_publication_dir",
    ),
]


class TestShouldSeedArchiveSample:
    @pytest.mark.parametrize("changed_path", trigger_path_cases)
    def test_returns_true_when_a_trigger_path_changed(self, changed_path: str):
        assert job.should_seed_archive_sample([changed_path]) is True

    def test_returns_false_when_only_unrelated_paths_changed(self):
        changed_paths = [
            "README.md",
            "projects/_07_workforce_characteristics/foo.py",
            "projects/_03_independent_cqc/_08_estimate_ind_cqc_filled_posts/foo.py",
            "terraform/pipeline/iam.tf",
            "terraform/pipeline/s3.tf",
            ".circleci/config.yml",
        ]

        assert job.should_seed_archive_sample(changed_paths) is False

    def test_returns_false_for_empty_changed_paths(self):
        assert job.should_seed_archive_sample([]) is False


class TestMain:
    def test_prints_true_when_dry_run_changed_path_matches_a_trigger(self, capsys):
        job.main(
            [
                "--changed-path",
                "projects/_03_independent_cqc/_09_archive_estimates/fargate/archive_job_role_estimates.py",
            ]
        )

        assert capsys.readouterr().out.strip() == "true"

    def test_prints_false_when_dry_run_changed_path_does_not_match_any_trigger(
        self, capsys
    ):
        job.main(["--changed-path", "CHANGELOG.md"])

        assert capsys.readouterr().out.strip() == "false"
