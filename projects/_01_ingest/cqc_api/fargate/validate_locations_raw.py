import sys

import pointblank as pb

from polars_utils import utils
from polars_utils import validate as vl
from polars_utils.logger import get_logger
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

logger = get_logger(__name__)

COMPLETE_COLUMNS = [CQCL.location_id, Keys.import_date, CQCL.name]
INDEX_COLUMNS = [CQCL.location_id, Keys.import_date]


def validate_dataset(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates a dataset according to a set of provided rules and produces a summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset and output the report to
            - shoud correspond to workspace / feature branch name
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """
    source_df = vl.read_parquet(f"""s3://{bucket_name}/{source_path.strip("/")}/""")

    validation = (
        pb.Validate(data=source_df, thresholds=pb.Thresholds(warning=1))
        .col_vals_not_null(COMPLETE_COLUMNS)
        .rows_distinct(INDEX_COLUMNS)
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path.strip("/"))


if __name__ == "__main__":
    logger.info(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    logger.info(f"Starting validation for {args.source_path}")

    validate_dataset(args.bucket_name, args.source_path, args.report_path)
    logger.info(f"Validation of {args.source_path} complete")
