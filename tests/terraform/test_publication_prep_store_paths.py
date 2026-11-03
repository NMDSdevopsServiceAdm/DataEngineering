import re
from dataclasses import dataclass
from pathlib import Path

import pytest

STEP_FUNCTIONS_DIR = (
    Path(__file__).parents[2] / "terraform" / "pipeline" / "step-functions" / "dynamic"
)

GLUE_TF_PATH = Path(__file__).parents[2] / "terraform" / "pipeline" / "glue.tf"

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
    Guards against the publication crawler cataloguing the assessment workbooks,
    which are .xlsx files written under the crawled publication domain.
    """

    def test_crawler_excludes_the_assessment_workbook_folder(self):
        [workbook_path] = set(
            get_command_paths("Publication-Assessment.json", "--workbook_destination")
        )
        workbook_folder = workbook_path.removeprefix(PUBLICATION_DOMAIN_PREFIX)

        assert (
            f"{workbook_folder.rstrip('/')}/**" in get_publication_crawler_exclusions()
        )
