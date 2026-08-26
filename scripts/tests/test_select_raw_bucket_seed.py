import pytest

import scripts.select_raw_bucket_seed as job

ALL_DOMAINS = list(job.DOMAIN_TRIGGER_PATHS)

# Includes paths that were trigger paths before this ticket's trigger list was
# trimmed, so a re-add wouldn't silently pass unnoticed. Also includes one
# non-trigger file per ingest domain (a downstream clean/validate-cleaned job,
# a test file, or a utils module) to guard against the trigger list widening
# back out to a directory prefix.
UNRELATED_PATHS = [
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
    "projects/_01_ingest/ons_pd/jobs/clean_ons_data.py",
]

domain_trigger_cases = [
    pytest.param(
        "ascwds",
        "projects/_01_ingest/ascwds/fargate/ingest_ascwds_dataset.py",
        id="returns_true_when_ascwds_ingest_job_changed",
    ),
    pytest.param(
        "ascwds",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_worker_raw_data.py",
        id="returns_true_when_ascwds_worker_raw_validate_changed",
    ),
    pytest.param(
        "ascwds",
        "projects/_01_ingest/ascwds/fargate/validate_ascwds_workplace_raw_data.py",
        id="returns_true_when_ascwds_workplace_raw_validate_changed",
    ),
    pytest.param(
        "capacity_tracker",
        "projects/_01_ingest/capacity_tracker/fargate/ingest_capacity_tracker_data.py",
        id="returns_true_when_capacity_tracker_ingest_job_changed",
    ),
    pytest.param(
        "cqc_pir",
        "projects/_01_ingest/cqc_pir/fargate/ingest_cqc_pir_data.py",
        id="returns_true_when_cqc_pir_ingest_job_changed",
    ),
    pytest.param(
        "cqc_pir",
        "projects/_01_ingest/cqc_pir/fargate/validate_cqc_pir_raw_data.py",
        id="returns_true_when_cqc_pir_raw_validate_changed",
    ),
    pytest.param(
        "ons_pd",
        "projects/_01_ingest/ons_pd/fargate/ingest_ons_data.py",
        id="returns_true_when_ons_pd_ingest_job_changed",
    ),
    pytest.param(
        "ons_pd",
        "projects/_01_ingest/ons_pd/fargate/validate_postcode_directory_raw_data.py",
        id="returns_true_when_ons_pd_raw_validate_changed",
    ),
]


class TestShouldSeedDomain:
    @pytest.mark.parametrize("domain, changed_path", domain_trigger_cases)
    def test_returns_true_when_domain_dir_changed(self, domain, changed_path):
        assert job.should_seed_domain(domain, [changed_path]) is True

    @pytest.mark.parametrize(
        "domain",
        ALL_DOMAINS,
        ids=[
            f"returns_true_when_eventbridge_terraform_changed_for_{d}"
            for d in ALL_DOMAINS
        ],
    )
    def test_returns_true_when_eventbridge_terraform_changed(self, domain):
        assert (
            job.should_seed_domain(domain, ["terraform/pipeline/eventbridge.tf"])
            is True
        )

    def test_returns_false_when_a_different_domains_dir_changed(self):
        # A change under ons_pd should not set capacity_tracker's flag -- the
        # whole point of splitting the gate per domain.
        changed_paths = ["projects/_01_ingest/ons_pd/jobs/ingest_ons_pd.py"]

        assert job.should_seed_domain("capacity_tracker", changed_paths) is False

    @pytest.mark.parametrize("domain", ALL_DOMAINS)
    def test_returns_false_when_cqc_api_dir_changed(self, domain):
        # cqc_api is a sibling ingest domain that never reads the raw bucket --
        # a prefix match here would wrongly widen the trigger set.
        changed_paths = ["projects/_01_ingest/cqc_api/jobs/ingest_cqc_api.py"]

        assert job.should_seed_domain(domain, changed_paths) is False

    @pytest.mark.parametrize("domain", ALL_DOMAINS)
    def test_returns_false_for_empty_changed_paths(self, domain):
        assert job.should_seed_domain(domain, []) is False

    @pytest.mark.parametrize("domain", ALL_DOMAINS)
    def test_returns_false_when_only_unrelated_paths_changed(self, domain):
        assert job.should_seed_domain(domain, UNRELATED_PATHS) is False


class TestMain:
    def test_main_prints_true_when_dry_run_changed_path_matches_the_given_domain(
        self, capsys
    ):
        job.main(
            [
                "--domain",
                "ascwds",
                "--changed-path",
                "projects/_01_ingest/ascwds/fargate/ingest_ascwds_dataset.py",
            ]
        )

        assert capsys.readouterr().out.strip() == "true"

    def test_main_prints_false_when_dry_run_changed_path_does_not_match_the_given_domain(
        self, capsys
    ):
        job.main(["--domain", "ascwds", "--changed-path", "CHANGELOG.md"])

        assert capsys.readouterr().out.strip() == "false"

    def test_main_exits_with_error_for_an_unknown_domain(self, capsys):
        with pytest.raises(SystemExit) as exc_info:
            job.main(
                ["--domain", "not-a-real-domain", "--changed-path", "CHANGELOG.md"]
            )

        assert exc_info.value.code == 2
