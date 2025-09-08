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


DATASETS_FILE = Path(__file__).parent.resolve() / "config" / "datasets.yml"
CONFIG = yaml.safe_load(DATASETS_FILE.read_text())


@dataclass
class DatasetConfig:
    dataset: str
    domain: str
    version: str
    report_name: str


def main(bucket_name: str, dataset: str):
    config = DatasetConfig(**CONFIG["datasets"][dataset])
    logging.info(f"Using dataset configuration: {config}")
    rules_yml = f"config/{dataset}.yml"

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
        for step in steps:
            if not step["all_passed"]:
                step_idx = step["i"]
                assertion = step["assertion_type"]
                _col_or_cols = step["column"]
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
        raise


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--dataset", "The dataset to validate"),
    )
    logger.info(f"Starting validation for {args.dataset}")

    main(args.bucket_name, args.dataset)
    logger.info(f"Validation of {args.dataset} complete")
