import sys

import pointblank as pb

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates raw CQC PIR data and produces a summary report plus failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name).
        source_path (str): the source dataset path to be validated.
        reports_path (str): the output path to write reports to.
    """
    raw_pir_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    validation = (
        pb.Validate(
            data=raw_pir_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # complete columns
        .col_vals_not_null(
            columns=[Keys.import_date, PIRCols.location_id],
            brief="Key columns should contain no null values",
        )
        # min/max value
        .col_vals_between(
            columns=[PIRCols.pir_people_directly_employed],
            left=0,
            right=1500,
            na_pass=True,
            brief=f"{PIRCols.pir_people_directly_employed} should be between 0 and 1500",
        ).interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
