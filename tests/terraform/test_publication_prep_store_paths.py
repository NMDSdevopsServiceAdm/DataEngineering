import re
from dataclasses import dataclass
from pathlib import Path

import pytest

STEP_FUNCTIONS_DIR = (
    Path(__file__).parents[2] / "terraform" / "pipeline" / "step-functions" / "dynamic"
)

GLUE_TF_PATH = Path(__file__).parents[2] / "terraform" / "pipeline" / "glue.tf"
S3_TF_PATH = Path(__file__).parents[2] / "terraform" / "pipeline" / "s3.tf"

DATASET_BUCKET_URI_PREFIX = "${dataset_bucket_uri}/"
PUBLICATION_DOMAIN_PREFIX = "domain=99_publication/"


def get_command_paths(step_function_filename: str, flag: str) -> list[str]:
    """
    Extracts every path passed to a flag in a step function's publication commands.

    A task given the bucket separately (e.g. validation) passes a path relative to
    the bucket, so the dataset bucket URI prefix is removed to compare paths.

    Args:
        step_function_filename (str): Filename under terraform/pipeline/step-functions/dynamic/.
        flag (str): The CLI flag whose values to extract, e.g. "--prep_source" or "--assessment_source".

    Returns:
        list[str]: The paths passed to that flag, in file order.
    """
    step_function_path = STEP_FUNCTIONS_DIR / step_function_filename
    flag_matches = re.findall(
        rf"'{re.escape(flag)}',\s*'([^']+)'", step_function_path.read_text()
    )
    assert flag_matches, f"Flag '{flag}' not found in {step_function_path}"
    return [path.removeprefix(DATASET_BUCKET_URI_PREFIX) for path in flag_matches]


@dataclass
class PrepStoreCommandTestCase:
    id: str
    step_function_filename: str
    flag: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(self.step_function_filename, self.flag, id=self.id)


prep_store_command_test_cases = [
    PrepStoreCommandTestCase(
        id="assessment_merge_and_prep_writes_the_store",
        step_function_filename="Publication-Assessment.json",
        flag="--prep_destination",
    ),
    PrepStoreCommandTestCase(
        id="assessment_reads_the_store",
        step_function_filename="Publication-Assessment.json",
        flag="--prep_source",
    ),
]


class TestPublicationPrepStorePathConsistency:
    """
    Guards against the prepared data store's path drifting between the task that
    writes it and the tasks that read it. A mismatch only shows up at runtime as
    "No archived runs found".
    """

    def test_every_command_uses_the_same_prep_store_path(self):
        paths = {
            case.id: get_command_paths(case.step_function_filename, case.flag)
            for case in prep_store_command_test_cases
        }

        assert (
            len({path for case_paths in paths.values() for path in case_paths}) == 1
        ), paths

    @pytest.mark.parametrize(
        "step_function_filename,flag",
        [case.as_pytest_param() for case in prep_store_command_test_cases],
    )
    def test_prep_store_path_is_the_merge_and_prep_dataset(
        self, step_function_filename, flag
    ):
        paths = get_command_paths(step_function_filename, flag)

        assert all(
            path.endswith("/dataset=monthly_tracker_filled_posts_01_merge_and_prep/")
            for path in paths
        ), paths


assessment_store_command_test_cases = [
    PrepStoreCommandTestCase(
        id="assessment_writes_the_store",
        step_function_filename="Publication-Assessment.json",
        flag="--assessment_destination",
    ),
    PrepStoreCommandTestCase(
        id="output_reads_the_store",
        step_function_filename="Publication-Output.json",
        flag="--assessment_source",
    ),
]


class TestPublicationAssessmentStorePathConsistency:
    """
    Guards against the assessed data store's path drifting between the task that
    writes it and the task that reads it.
    """

    def test_every_command_uses_the_same_assessment_store_path(self):
        paths = {
            case.id: get_command_paths(case.step_function_filename, case.flag)
            for case in assessment_store_command_test_cases
        }

        assert (
            len({path for case_paths in paths.values() for path in case_paths}) == 1
        ), paths

    @pytest.mark.parametrize(
        "step_function_filename,flag",
        [case.as_pytest_param() for case in assessment_store_command_test_cases],
    )
    def test_assessment_store_path_is_the_assessment_dataset(
        self, step_function_filename, flag
    ):
        paths = get_command_paths(step_function_filename, flag)

        assert all(
            path.endswith("/dataset=monthly_tracker_filled_posts_02_assessment/")
            for path in paths
        ), paths


def get_publication_crawler_exclusions() -> list[str]:
    """
    Extracts the exclusion patterns of the publication crawler in glue.tf.

    Returns:
        list[str]: The patterns, relative to the crawler's domain folder.
    """
    glue_tf = GLUE_TF_PATH.read_text()
    module_block = re.search(r'module "publication_crawler" \{(.*?)\n\}', glue_tf, re.S)
    assert module_block, f"publication_crawler not found in {GLUE_TF_PATH}"
    exclusions = re.search(r"exclusions\s*=\s*\[(.*?)\]", module_block.group(1), re.S)
    return re.findall(r'"([^"]+)"', exclusions.group(1)) if exclusions else []


class TestPublicationCrawlerExclusions:
    """
    Guards against the publication crawler cataloguing the assessment and data
    download workbooks, which are .xlsx files written under the crawled
    publication domain.
    """

    @pytest.mark.parametrize(
        "step_function_filename",
        ["Publication-Assessment.json", "Publication-Output.json"],
        ids=["assessment_workbook", "data_download_workbook"],
    )
    def test_crawler_excludes_the_workbook_folder(self, step_function_filename):
        [workbook_path] = set(
            get_command_paths(step_function_filename, "--workbook_destination")
        )
        workbook_folder = workbook_path.removeprefix(PUBLICATION_DOMAIN_PREFIX)

        assert (
            f"{workbook_folder.rstrip('/')}/**" in get_publication_crawler_exclusions()
        )


@dataclass
class ExpirationRule:
    id: str
    prefix: str
    expiration_days: int


def get_datasets_bucket_expiration_rules() -> list[ExpirationRule]:
    """
    Extracts the expiration rules of the datasets bucket in s3.tf.

    Returns:
        list[ExpirationRule]: The rules, in file order.
    """
    s3_tf = S3_TF_PATH.read_text()
    module_block = re.search(r'module "datasets_bucket" \{(.*?)\n\}', s3_tf, re.S)
    assert module_block, f"datasets_bucket not found in {S3_TF_PATH}"
    rules = [
        ExpirationRule(rule_id, prefix, int(days))
        for rule_id, prefix, days in re.findall(
            r'id\s*=\s*"([^"]+)"\s*prefix\s*=\s*"([^"]+)"\s*expiration_days\s*=\s*(\d+)',
            module_block.group(1),
        )
    ]
    assert rules, f"No expiration rules parsed from {S3_TF_PATH}"
    return rules


store_expiry_test_cases = [
    PrepStoreCommandTestCase(
        id="expires_the_prep_store",
        step_function_filename="Publication-Assessment.json",
        flag="--prep_destination",
    ),
    PrepStoreCommandTestCase(
        id="expires_the_assessment_store",
        step_function_filename="Publication-Assessment.json",
        flag="--assessment_destination",
    ),
    PrepStoreCommandTestCase(
        id="expires_the_validation_report_folders",
        step_function_filename="Publication-Assessment.json",
        flag="--reports_path",
    ),
    PrepStoreCommandTestCase(
        id="expires_the_assessment_workbook_folder",
        step_function_filename="Publication-Assessment.json",
        flag="--workbook_destination",
    ),
    PrepStoreCommandTestCase(
        id="expires_the_output_workbook_folder",
        step_function_filename="Publication-Output.json",
        flag="--workbook_destination",
    ),
]


class TestPublicationStoreExpiry:
    """
    Guards the datasets bucket's expiry rules against drifting from the paths the
    publication jobs write to.
    """

    @pytest.mark.parametrize(
        "step_function_filename,flag",
        [case.as_pytest_param() for case in store_expiry_test_cases],
    )
    def test_store_is_expired(self, step_function_filename, flag):
        rule_prefixes = {rule.prefix for rule in get_datasets_bucket_expiration_rules()}

        assert set(get_command_paths(step_function_filename, flag)) <= rule_prefixes

    def test_publication_stores_expire_after_60_days(self):
        publication_rules = [
            rule
            for rule in get_datasets_bucket_expiration_rules()
            if rule.prefix.startswith(PUBLICATION_DOMAIN_PREFIX)
        ]

        assert len(publication_rules) == 6
        assert {rule.expiration_days for rule in publication_rules} == {60}

    def test_output_dataset_is_not_expired(self):
        # Un-partitioned and overwritten each run: an age rule would delete the
        # current output.
        [output_path] = set(
            get_command_paths("Publication-Output.json", "--output_destination")
        )

        assert not any(
            output_path.startswith(rule.prefix)
            for rule in get_datasets_bucket_expiration_rules()
        )

    def test_every_expiry_prefix_ends_with_a_slash(self):
        # Without the slash a prefix also matches sibling datasets sharing its
        # name, e.g. _03_output would catch _03_output_workbook.
        assert all(
            rule.prefix.endswith("/") for rule in get_datasets_bucket_expiration_rules()
        )


class TestPublicationOutputWorkbookPaths:
    """
    Guards the data download workbook's paths between the task that writes it
    and the task that reconciles it, and the logo the workbook needs.
    """

    def test_build_output_and_compare_totals_use_the_same_workbook_path(self):
        written = get_command_paths("Publication-Output.json", "--workbook_destination")
        read = get_command_paths("Publication-Output.json", "--workbook_source")

        assert len(set(written)) == 1, written
        assert set(written) == set(read), (written, read)

    def test_build_output_and_compare_totals_use_the_same_output_store_path(self):
        written = get_command_paths("Publication-Output.json", "--output_destination")
        read = get_command_paths("Publication-Output.json", "--output_source")

        assert len(set(written)) == 1, written
        assert set(written) == set(read), (written, read)

    def test_build_output_passes_a_png_logo_source(self):
        [logo_source] = get_command_paths("Publication-Output.json", "--logo_source")

        assert logo_source.startswith("${pipeline_resources_bucket_uri}/"), logo_source
        assert logo_source.endswith(".png"), logo_source
