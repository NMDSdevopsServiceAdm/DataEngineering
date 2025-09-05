import logging
import sys

import boto3
import pointblank as pb
import polars as pl

from polars_utils import utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(bucket_name: str, domain: str, dataset: str, version: str, report_name: str):
    rules_yml = f"rules/{dataset}.yml"

    source = f"s3://{bucket_name}/domain={domain}/dataset={dataset}/version={version}/"
    destination = (
        f"s3://{bucket_name}/domain=data_validation_reports/dataset={report_name}/"
    )

    dataframe = pl.scan_parquet(
        source,
        cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        extra_columns="ignore",
    ).collect()

    pb.validate_yaml(rules_yml)
    validation = pb.yaml_interrogate(rules_yml, set_tbl=dataframe)
    report = validation.get_tabular_report()
    # report.show()

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=report.as_raw_html(inline_css=True, make_page=True),
        Bucket=bucket_name,
        Key=f"{destination}/index.html",
    )
    validation.assert_below_threshold(level="warning")


if __name__ == "__main__":
    logger.info("Spark job 'validate_locations_api_raw_data' starting...")
    logger.info(f"Job parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--domain", "Domain of the dataset to determine s3 source location"),
        ("--dataset", "The dataset to validate"),
        ("--version", "Dataset version", "3.0.0"),
        ("--report_name", "Name of the report to determine s3 directory"),
    )
    main(args.bucket_name, args.domain, args.dataset, args.version, args.report_name)
    logger.info("ECS task 'validate_locations_api_raw_data' complete")
