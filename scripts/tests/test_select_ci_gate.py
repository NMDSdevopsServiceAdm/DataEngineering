from pathlib import Path

import pytest

import scripts.select_ci_gate as job

RAW_BUCKET_DOMAINS = ["ascwds", "capacity_tracker", "cqc_pir", "ons_pd"]

# Includes a non-trigger file beside each domain's triggers, so the list can't
# silently widen back to a directory prefix.
RAW_BUCKET_UNRELATED_PATHS = [
    "README.md",
    "projects/_07_workforce_characteristics/foo.py",
    "terraform/pipeline/iam.tf",
    "terraform/pipeline/s3.tf",
    "terraform/modules/fargate-task/iam.tf",
    ".circleci/config.yml",
    "projects/_01_ingest/ascwds/fargate/clean_ascwds_workplace.py",
    "projects/_01_ingest/ascwds/tests/jobs/test_ingest_ascwds_dataset.py",
    "projects/_01_ingest/capacity_tracker/jobs/clean_capacity_tracker_care_home_data.py",
    "projects/_01_ingest/cqc_pir/utils/null_people_directly_employed_outliers.py",
    "projects/_01_ingest/ons_pd/fargate/clean_ons_data.py",
]

# Paths that must not trigger each gate.
UNRELATED_PATHS: dict[str, list[str]] = {
    **{
        f"raw-bucket-{domain}": RAW_BUCKET_UNRELATED_PATHS
        for domain in RAW_BUCKET_DOMAINS
    },
    "archive-sample": [
        "README.md",
        "projects/_07_workforce_characteristics/foo.py",
        "projects/_03_independent_cqc/_08_estimate_ind_cqc_filled_posts/foo.py",
        "terraform/pipeline/iam.tf",
        "terraform/pipeline/s3.tf",
        ".circleci/config.yml",
    ],
    "cqc-integration-tests": [
        "README.md",
        "projects/_07_workforce_characteristics/foo.py",
        "terraform/pipeline/iam.tf",
        ".circleci/config.yml",
        "projects/_01_ingest/ascwds/fargate/clean_ascwds_workplace.py",
        "projects/_01_ingest/cqc_pir/utils/null_people_directly_employed_outliers.py",
        "utils/column_names/raw_data_files/cqc_pir_columns.py",
    ],
}

ALL_GATES = list(job.GATE_TRIGGER_PATHS)
REPO_ROOT = Path(job.__file__).resolve().parent.parent

trigger_cases = [
    pytest.param(
        "raw-bucket-ascwds",
        "projects/_01_ingest/ascwds/fargate/ingest_ascwds_dataset.py",
        id="returns_true_when_ascwds_ingest_job_changed",
    ),
    pytest.param(
        "raw-bucket-ascwds",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_worker_raw_data.py",
        id="returns_true_when_ascwds_worker_raw_validate_changed",
    ),
    pytest.param(
        "raw-bucket-ascwds",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_workplace_raw_data.py",
        id="returns_true_when_ascwds_workplace_raw_validate_changed",
    ),
    pytest.param(
        "raw-bucket-capacity_tracker",
        "projects/_01_ingest/capacity_tracker/fargate/ingest_capacity_tracker_data.py",
        id="returns_true_when_capacity_tracker_ingest_job_changed",
    ),
    pytest.param(
        "raw-bucket-cqc_pir",
        "projects/_01_ingest/cqc_pir/fargate/ingest_cqc_pir_data.py",
        id="returns_true_when_cqc_pir_ingest_job_changed",
    ),
    pytest.param(
        "raw-bucket-cqc_pir",
        "projects/_01_ingest/cqc_pir/fargate/validate_cqc_pir_raw_data.py",
        id="returns_true_when_cqc_pir_raw_validate_changed",
    ),
    pytest.param(
        "raw-bucket-cqc_pir",
        "projects/_01_ingest/cqc_pir/fargate/clean_cqc_pir_data.py",
        id="returns_true_when_cqc_pir_clean_job_changed",
    ),
    pytest.param(
        "raw-bucket-cqc_pir",
        "projects/_01_ingest/cqc_pir/fargate/validate_clean_cqc_pir_data.py",
        id="returns_true_when_cqc_pir_clean_validate_changed",
    ),
    pytest.param(
        "raw-bucket-ons_pd",
        "projects/_01_ingest/ons_pd/fargate/ingest_ons_data.py",
        id="returns_true_when_ons_pd_ingest_job_changed",
    ),
    pytest.param(
        "raw-bucket-ons_pd",
        "projects/_01_ingest/ons_pd/fargate/validate_postcode_directory_raw_data.py",
        id="returns_true_when_ons_pd_raw_validate_changed",
    ),
    *[
        pytest.param(
            f"raw-bucket-{domain}",
            "terraform/pipeline/eventbridge.tf",
            id=f"returns_true_when_eventbridge_terraform_changed_for_{domain}",
        )
        for domain in RAW_BUCKET_DOMAINS
    ],
    pytest.param(
        "archive-sample",
        "projects/_03_independent_cqc/_01_filled_posts/_07_archive/fargate/archive_job_role_estimates.py",
        id="returns_true_when_changed_path_is_under_archive_estimates_dir",
    ),
    pytest.param(
        "archive-sample",
        "projects/_03_independent_cqc/_01_filled_posts/_07_archive/fargate/utils/archive_utils.py",
        id="returns_true_when_changed_path_is_under_archive_estimates_utils_dir",
    ),
    pytest.param(
        "archive-sample",
        "projects/_99_publication/monthly_tracker_filled_posts/fargate/_01_merge.py",
        id="returns_true_when_changed_path_is_under_publication_dir",
    ),
    pytest.param(
        "cqc-integration-tests",
        "projects/_01_ingest/cqc_api/utils/cqc_api.py",
        id="returns_true_when_cqc_api_client_changed",
    ),
    pytest.param(
        "cqc-integration-tests",
        "projects/_01_ingest/cqc_api/fargate/cqc_locations_1_delta_api_download.py",
        id="returns_true_when_cqc_api_fargate_job_changed",
    ),
    pytest.param(
        "cqc-integration-tests",
        "tests/integration/test_cqc_api_integration.py",
        id="returns_true_when_the_integration_test_file_itself_changed",
    ),
    pytest.param(
        "cqc-integration-tests",
        "utils/column_names/raw_data_files/cqc_location_api_columns.py",
        id="returns_true_when_location_columns_changed",
    ),
    pytest.param(
        "cqc-integration-tests",
        "utils/column_names/raw_data_files/cqc_provider_api_columns.py",
        id="returns_true_when_provider_columns_changed",
    ),
    pytest.param(
        "cqc-integration-tests",
        "utils/aws_secrets_manager_utilities.py",
        id="returns_true_when_secrets_manager_utility_changed",
    ),
]


class TestGateTriggered:
    @pytest.mark.parametrize("gate, changed_path", trigger_cases)
    def test_returns_true_when_a_trigger_path_changed(
        self, gate: str, changed_path: str
    ):
        assert job.gate_triggered(gate, [changed_path]) is True

    def test_returns_false_when_a_different_domains_dir_changed(self):
        # A change under ons_pd should not set capacity_tracker's flag -- the
        # whole point of splitting the raw bucket gate per domain.
        changed_paths = ["projects/_01_ingest/ons_pd/jobs/ingest_ons_pd.py"]

        assert job.gate_triggered("raw-bucket-capacity_tracker", changed_paths) is False

    @pytest.mark.parametrize("domain", RAW_BUCKET_DOMAINS)
    def test_returns_false_when_cqc_api_dir_changed(self, domain: str):
        # cqc_api is a sibling ingest domain that never reads the raw bucket --
        # a prefix match here would wrongly widen the trigger set.
        changed_paths = ["projects/_01_ingest/cqc_api/jobs/ingest_cqc_api.py"]

        assert job.gate_triggered(f"raw-bucket-{domain}", changed_paths) is False

    @pytest.mark.parametrize("gate", ALL_GATES)
    def test_returns_false_for_empty_changed_paths(self, gate: str):
        assert job.gate_triggered(gate, []) is False

    @pytest.mark.parametrize("gate", ALL_GATES)
    def test_returns_false_when_only_unrelated_paths_changed(self, gate: str):
        assert job.gate_triggered(gate, UNRELATED_PATHS[gate]) is False

    @pytest.mark.parametrize("gate", ALL_GATES)
    def test_every_trigger_path_exists_in_repo(self, gate: str):
        # A renamed or deleted trigger file would otherwise leave the gate
        # silently never firing. Globbed so a `*` trigger path is covered too.
        missing = [
            trigger_path
            for trigger_path in job.GATE_TRIGGER_PATHS[gate]
            if not any(REPO_ROOT.glob(trigger_path))
        ]

        assert missing == []


class TestMain:
    def test_prints_true_when_dry_run_changed_path_matches_the_given_gate(self, capsys):
        job.main(
            [
                "--gate",
                "raw-bucket-ascwds",
                "--changed-path",
                "projects/_01_ingest/ascwds/fargate/ingest_ascwds_dataset.py",
            ]
        )

        assert capsys.readouterr().out.strip() == "true"

    def test_prints_false_when_dry_run_changed_path_does_not_match_the_given_gate(
        self, capsys
    ):
        job.main(["--gate", "archive-sample", "--changed-path", "CHANGELOG.md"])

        assert capsys.readouterr().out.strip() == "false"

    def test_exits_with_error_for_an_unknown_gate(self):
        with pytest.raises(SystemExit) as exc_info:
            job.main(["--gate", "not-a-real-gate", "--changed-path", "CHANGELOG.md"])

        assert exc_info.value.code == 2

    def test_exits_with_error_when_gate_is_omitted(self):
        with pytest.raises(SystemExit) as exc_info:
            job.main(["--changed-path", "CHANGELOG.md"])

        assert exc_info.value.code == 2
