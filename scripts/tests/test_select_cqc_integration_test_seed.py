import pytest

import scripts.select_cqc_integration_test_seed as job

UNRELATED_PATHS = [
    "README.md",
    "projects/_07_workforce_characteristics/foo.py",
    "terraform/pipeline/iam.tf",
    ".circleci/config.yml",
    "projects/_01_ingest/ascwds/fargate/clean_ascwds_workplace.py",
    "projects/_01_ingest/cqc_pir/utils/null_people_directly_employed_outliers.py",
    "utils/column_names/raw_data_files/cqc_pir_columns.py",
]

trigger_cases = [
    pytest.param(
        "projects/_01_ingest/cqc_api/utils/cqc_api.py",
        id="returns_true_when_cqc_api_client_changed",
    ),
    pytest.param(
        "projects/_01_ingest/cqc_api/fargate/cqc_locations_1_delta_api_download.py",
        id="returns_true_when_cqc_api_fargate_job_changed",
    ),
    pytest.param(
        "tests/integration/test_cqc_api_integration.py",
        id="returns_true_when_the_integration_test_file_itself_changed",
    ),
    pytest.param(
        "utils/column_names/raw_data_files/cqc_location_api_columns.py",
        id="returns_true_when_location_columns_changed",
    ),
    pytest.param(
        "utils/column_names/raw_data_files/cqc_provider_api_columns.py",
        id="returns_true_when_provider_columns_changed",
    ),
    pytest.param(
        "utils/aws_secrets_manager_utilities.py",
        id="returns_true_when_secrets_manager_utility_changed",
    ),
]


class TestShouldRunCqcIntegrationTests:
    @pytest.mark.parametrize("changed_path", trigger_cases)
    def test_returns_true_when_a_trigger_path_changed(self, changed_path):
        assert job.should_run_cqc_integration_tests([changed_path]) is True

    def test_returns_false_for_empty_changed_paths(self):
        assert job.should_run_cqc_integration_tests([]) is False

    def test_returns_false_when_only_unrelated_paths_changed(self):
        assert job.should_run_cqc_integration_tests(UNRELATED_PATHS) is False


class TestMain:
    def test_main_prints_true_when_dry_run_changed_path_is_a_trigger(self, capsys):
        job.main(
            [
                "--changed-path",
                "projects/_01_ingest/cqc_api/utils/cqc_api.py",
            ]
        )

        assert capsys.readouterr().out.strip() == "true"

    def test_main_prints_false_when_dry_run_changed_path_is_not_a_trigger(self, capsys):
        job.main(["--changed-path", "CHANGELOG.md"])

        assert capsys.readouterr().out.strip() == "false"
