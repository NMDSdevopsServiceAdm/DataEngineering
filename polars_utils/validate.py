import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import boto3
import pointblank as pb
import polars as pl
import yaml

from polars_utils import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# this is the master config for datasets specification
DATASETS_FILE = Path(__file__).parent.resolve() / "config" / "datasets.yml"
CONFIG = yaml.safe_load(DATASETS_FILE.read_text())


@dataclass
class DatasetConfig:
    dataset: str
    domain: str
    version: str
    report_name: str


def validate_dataset(bucket_name: str, dataset: str):
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    NB: this validatation is config-driven so only requires additional YAML configuration to entend to various datasets.

    See `config/README.md` for further details.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        dataset (str): the dataset name as the source data to be validated

    Raises:
        ValueError: in case of a missing dataset key in the `config/datasets.yml`
        FileNotFoundError: in case of a missing rules definition (eg. `config/dataset_name.yml`) for the dataset validation
        AssertionError: in case of the dataset failing the validation rules
    """
    # each dataset validation requires a config entry in the master config file
    if dataset not in CONFIG["datasets"]:
        raise ValueError(f"Dataset {dataset} not found in config file")
    config = DatasetConfig(**CONFIG["datasets"][dataset])
    logging.info(f"Using dataset configuration: {config}")

    rules_yml = f"config/{dataset}.yml"
    if not Path(rules_yml).exists():
        raise FileNotFoundError(f"Rules file {rules_yml} not found")

    source = f"s3://{bucket_name}/domain={config.domain}/dataset={config.dataset}/version={config.version}/"
    destination = f"domain=data_validation_reports/dataset={config.report_name}"

    dataframe = pl.scan_parquet(
        source,
        cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        extra_columns="ignore",
    ).collect()

    pb.validate_yaml(rules_yml)
    validation = pb.yaml_interrogate(rules_yml, set_tbl=dataframe)
    report = validation.get_tabular_report()

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=report.as_raw_html(inline_css=True, make_page=True),
        Bucket=bucket_name,
        Key=f"{destination}/index.html",
    )
    try:
        validation.assert_below_threshold(level="warning")
    except AssertionError:
        logger.error("Data validation failed. See report for details.")
        steps = json.loads(validation.get_json_report())
        # JSON report includes a detailed list of each validation step, including failures
        # Note that some 'steps' result in several steps in teh execution, eg. a null check over several columns
        for step in steps:
            if not step["all_passed"]:
                step_idx = step["i"]
                assertion = step["assertion_type"]
                _col_or_cols = step[
                    "column"
                ]  # could be a string or a list, depending on step specification
                columns = (
                    "_".join(_col_or_cols)
                    if isinstance(_col_or_cols, list)
                    else _col_or_cols
                )
                failed_records_df = validation.get_data_extracts(step_idx, frame=True)
                utils.write_to_parquet(
                    failed_records_df,
                    f"s3://{bucket_name}/{destination}/failed_step_{step_idx}_{assertion}_{columns}.parquet",
                )
        raise  # ensures that the task fails if any warnings / errors


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--dataset", "The dataset to validate"),
    )
    logger.info(f"Starting validation for {args.dataset}")

    validate_dataset(args.bucket_name, args.dataset)
    logger.info(f"Validation of {args.dataset} complete")
