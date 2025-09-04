import logging
import sys
from dataclasses import dataclass

import boto3
import pointblank as pb
import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class Rules:
    complete_columns = [
        CQCL.location_id,
        Keys.import_date,
        CQCL.name,
    ]
    index_columns = [
        CQCL.location_id,
        Keys.import_date,
    ]


def main(bucket_name: str, dataset_source: str, report_destination: str):
    raw_location_df = pl.scan_parquet(
        f"s3://{bucket_name}/{dataset_source}/",
        cast_options=pl.ScanCastOptions(missing_struct_fields="insert"),
        extra_columns="ignore",
    ).collect()

    validation = (
        pb.Validate(
            raw_location_df,
            thresholds=pb.Thresholds(warning=1),
            actions=pb.Actions(
                warning=logging.warning(
                    "{LEVEL}: {type} threshold exceeded for column {col}."
                )
            ),
            tbl_name="delta_locations_api",
        )
        .col_vals_not_null(
            Rules.complete_columns,
        )
        .rows_distinct(Rules.index_columns)
        .interrogate()
    )
    report = validation.get_tabular_report()

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=report.as_raw_html(inline_css=True, make_page=True),
        Bucket=bucket_name,
        Key=f"{report_destination}/full_report.html",
    )
    validation.assert_below_threshold(level="warning")


if __name__ == "__main__":
    logger.info("Spark job 'validate_locations_api_raw_data' starting...")
    logger.info(f"Job parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket name for dataset source and report destination"),
        ("--raw_dataset_source", "Dataset source path in S3"),
        ("--report_destination", "Destination path in S3 for the report"),
    )
    main(args.bucket_name, args.raw_dataset_source, args.report_destination)
    logger.info("ECS task 'validate_locations_api_raw_data' complete")
