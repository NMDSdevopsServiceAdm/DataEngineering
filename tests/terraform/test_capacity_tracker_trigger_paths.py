import re
from dataclasses import dataclass
from pathlib import Path

import pytest

TERRAFORM_PIPELINE_DIR = Path(__file__).parents[2] / "terraform" / "pipeline"
EVENTBRIDGE_TF = TERRAFORM_PIPELINE_DIR / "eventbridge.tf"
STEP_FUNCTIONS_DIR = TERRAFORM_PIPELINE_DIR / "step-functions" / "dynamic"


def get_trigger_key_prefix(event_rule_name: str) -> str:
    """
    Extracts an S3 upload event rule's key prefix from eventbridge.tf.

    Args:
        event_rule_name (str): The aws_cloudwatch_event_rule resource name, e.g.
            "ct_care_home_csv_added".

    Returns:
        str: The S3 object key prefix the rule filters on.
    """
    eventbridge_tf = EVENTBRIDGE_TF.read_text()
    rule_block = re.search(
        rf'resource "aws_cloudwatch_event_rule" "{event_rule_name}" {{.*?\n}}\n',
        eventbridge_tf,
        re.DOTALL,
    )
    prefix_match = re.search(r'"prefix":\s*"([^"]+)"', rule_block.group())
    return prefix_match.group(1)


def get_clean_step_source_path(step_function_filename: str, source_flag: str) -> str:
    """
    Extracts the clean step's source path argument from a step function definition.

    Args:
        step_function_filename (str): Filename under terraform/pipeline/step-functions/dynamic/.
        source_flag (str): The CLI flag whose value to extract, e.g.
            "--capacity_tracker_care_home_source".

    Returns:
        str: The source path passed to that flag.
    """
    step_function_json = (STEP_FUNCTIONS_DIR / step_function_filename).read_text()
    source_match = re.search(
        rf'"{re.escape(source_flag)}",\s*"([^"]+)"', step_function_json
    )
    return source_match.group(1)


def get_dataset_name(s3_path: str) -> str:
    """
    Extracts the dataset segment's name from an S3 path or key.

    Args:
        s3_path (str): An S3 path or key containing a "dataset=<name>" segment.

    Returns:
        str: The dataset name.
    """
    return re.search(r"dataset=([^/]+)", s3_path).group(1)


@dataclass
class TriggerPathConsistencyTestCase:
    id: str
    event_rule_name: str
    step_function_filename: str
    source_flag: str

    def as_pytest_param(self):
        """Return test case as pytest ParameterSet."""
        return pytest.param(
            self.event_rule_name,
            self.step_function_filename,
            self.source_flag,
            id=self.id,
        )


trigger_path_consistency_test_cases = [
    TriggerPathConsistencyTestCase(
        id="care_home_trigger_prefix_matches_clean_step_source_dataset",
        event_rule_name="ct_care_home_csv_added",
        step_function_filename="Ingest-Capacity-Tracker-Care-Home.json",
        source_flag="--capacity_tracker_care_home_source",
    ),
    TriggerPathConsistencyTestCase(
        id="non_res_trigger_prefix_matches_clean_step_source_dataset",
        event_rule_name="ct_non_res_csv_added",
        step_function_filename="Ingest-Capacity-Tracker-Non-Res.json",
        source_flag="--capacity_tracker_non_res_source",
    ),
]


class TestCapacityTrackerTriggerPathConsistency:
    """
    Guards against eventbridge.tf's S3 upload trigger drifting from the raw dataset
    name that ingest actually writes to (and clean reads from).

    Capacity Tracker's ingest job derives its whole output path from the raw upload's
    own S3 key, not from the destination argument it's given, so if the S3 trigger's
    key prefix and the clean step's source path disagree on the dataset name, the
    trigger silently stops firing for real uploads (see ticket 2111).
    """

    @pytest.mark.parametrize(
        "event_rule_name,step_function_filename,source_flag",
        [case.as_pytest_param() for case in trigger_path_consistency_test_cases],
    )
    def test_trigger_prefix_dataset_name_is_consistent_with_clean_step_source_dataset_name(
        self, event_rule_name, step_function_filename, source_flag
    ):
        trigger_prefix = get_trigger_key_prefix(event_rule_name)
        clean_step_source = get_clean_step_source_path(
            step_function_filename, source_flag
        )

        trigger_dataset_name = get_dataset_name(trigger_prefix)
        clean_step_dataset_name = get_dataset_name(clean_step_source)

        assert clean_step_dataset_name.startswith(trigger_dataset_name), (
            f"eventbridge.tf's {event_rule_name} trigger expects dataset "
            f"'{trigger_dataset_name}', but {step_function_filename}'s clean step reads "
            f"dataset '{clean_step_dataset_name}'. Ingest writes wherever the raw "
            "upload's own key says, so a mismatch here means the S3 trigger can no "
            "longer fire for real uploads."
        )
