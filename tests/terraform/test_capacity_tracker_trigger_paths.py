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

    Finds the resource block by its start marker and the next "resource" block
    (or end of file), rather than trying to match its closing brace, since the
    block contains a nested JSON heredoc with its own braces.

    Args:
        event_rule_name (str): The aws_cloudwatch_event_rule resource name, e.g.
            "ct_care_home_csv_added".

    Returns:
        str: The S3 object key prefix the rule filters on.
    """
    eventbridge_tf = EVENTBRIDGE_TF.read_text()
    block_start = re.search(
        rf'resource "aws_cloudwatch_event_rule" "{event_rule_name}" {{', eventbridge_tf
    )
    assert (
        block_start
    ), f"No aws_cloudwatch_event_rule named '{event_rule_name}' found in {EVENTBRIDGE_TF}"

    next_block_start = eventbridge_tf.find("\nresource ", block_start.end())
    rule_block = eventbridge_tf[
        block_start.end() : next_block_start if next_block_start != -1 else None
    ]

    prefix_match = re.search(r'"prefix":\s*"([^"]+)"', rule_block)
    assert (
        prefix_match
    ), f"No key prefix found in the '{event_rule_name}' rule block in {EVENTBRIDGE_TF}"
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
    step_function_path = STEP_FUNCTIONS_DIR / step_function_filename
    step_function_json = step_function_path.read_text()
    source_match = re.search(
        rf'"{re.escape(source_flag)}",\s*"([^"]+)"', step_function_json
    )
    assert source_match, f"Flag '{source_flag}' not found in {step_function_path}"
    return source_match.group(1)


def get_dataset_name(s3_path: str) -> str:
    """
    Extracts the dataset segment's name from an S3 path or key.

    Args:
        s3_path (str): An S3 path or key containing a "dataset=<name>" segment.

    Returns:
        str: The dataset name.
    """
    dataset_match = re.search(r"dataset=([^/]+)", s3_path)
    assert dataset_match, f"No 'dataset=' segment found in '{s3_path}'"
    return dataset_match.group(1)


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
    def test_trigger_and_clean_step_agree_on_dataset_name(
        self, event_rule_name, step_function_filename, source_flag
    ):
        trigger_prefix = get_trigger_key_prefix(event_rule_name)
        clean_step_source = get_clean_step_source_path(
            step_function_filename, source_flag
        )

        trigger_dataset_name = get_dataset_name(trigger_prefix)
        clean_step_dataset_name = get_dataset_name(clean_step_source)

        # startswith, not ==: EventBridge itself matches by key *prefix*, so a
        # deliberately-abbreviated trigger prefix (e.g. ASCWDS's "worker" vs a
        # longer real dataset name) is valid, not a bug.
        assert clean_step_dataset_name.startswith(trigger_dataset_name), (
            f"eventbridge.tf's {event_rule_name} trigger expects dataset "
            f"'{trigger_dataset_name}', but {step_function_filename}'s clean step reads "
            f"dataset '{clean_step_dataset_name}'. Ingest writes wherever the raw "
            "upload's own key says, so a mismatch here means the S3 trigger can no "
            "longer fire for real uploads."
        )
