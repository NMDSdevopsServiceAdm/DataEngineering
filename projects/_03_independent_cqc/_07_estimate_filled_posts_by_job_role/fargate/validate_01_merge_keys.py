import gc
import sys
from datetime import date

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

KEY_COLS = [
    IndCqcColumns.location_id,
    IndCqcColumns.cqc_location_import_date,
    IndCqcColumns.main_job_role_clean_labelled,
]


VALIDATE_KWARGS = dict(
    thresholds=GLOBAL_THRESHOLDS,
    brief=True,
    actions=GLOBAL_ACTIONS,
)


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        reports_path (str): the output path to write reports to
    """

    # can change name of this. I named it key validation initially because it started with just locartionid and import date validation
    run_distinct_key_validation(source_path, bucket_name, reports_path)
    gc.collect()


def convert_main_job_role_to_int(df: pl.DataFrame) -> pl.DataFrame:
    """Converts the main job role column to int for validation purposes."""
    return df.with_columns(
        pl.col(IndCqcColumns.main_job_role_clean_labelled).replace_strict(
            old=AscwdsWorkerValueLabelsMainjrid.labels_dict.values(),
            new=AscwdsWorkerValueLabelsMainjrid.labels_dict.keys(),
        )
    ).cast(pl.UInt8)


def run_distinct_key_validation(source_path, bucket_name, reports_path):
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=KEY_COLS,
    ).with_columns(pl.col(IndCqcColumns.main_job_role_clean_labelled).cast(pl.String))

    distinct_key_validation = (
        pb.Validate(
            data=source_df,
            label=f"Distinct key validation of {source_path}",
            **VALIDATE_KWARGS,
        )
        .rows_distinct(
            pre=convert_main_job_role_to_int,
            columns_subset=[
                IndCqcColumns.location_id,
                IndCqcColumns.cqc_location_import_date,
                IndCqcColumns.main_job_role_clean_labelled,
            ],
            brief="Primary key (location_id, cqc_location_import_date, main_job_role_clean_labelled) should be unique",
        )
        .interrogate()
    )
    vl.write_reports(
        distinct_key_validation, bucket_name, f"{reports_path}distinct_key/"
    )
    del source_df, distinct_key_validation


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
