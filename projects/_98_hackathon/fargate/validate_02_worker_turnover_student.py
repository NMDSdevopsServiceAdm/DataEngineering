import sys

import pointblank as pb
import polars as pl

import projects._98_hackathon.fargate.utils.validation_utils as vUtils
from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols

KEY_COLUMNS = [
    AWKClean.establishment_id,
    AWKClean.worker_id,
    AWKClean.ascwds_worker_import_date,
]

TEST_SPLIT = "test"


def main(
    bucket_name: str,
    source_path: str,
    test_path: str,
    reports_path: str,
) -> None:
    """Validates the student version of the test split and writes the reports.

    Every column of the student data is read, as the checks include that the
    hidden columns are absent. The student data is a small split of the dataset.

    Args:
        bucket_name (str): the bucket (name only) for the datasets and reports
        source_path (str): the student worker turnover dataset path to validate
        test_path (str): the worker turnover test split dataset path; its row
            count is the expected row count
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")
    expected_row_count = (
        utils.scan_parquet(
            f"s3://{bucket_name}/{test_path}", selected_columns=[HackCols.split]
        )
        .select(pl.len())
        .collect()
        .item()
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows (same as the test split)",
        )
        .col_vals_not_null(KEY_COLUMNS, brief="key columns have no nulls")
        .rows_distinct(
            KEY_COLUMNS,
            brief="establishment, worker and import date are unique",
        )
        .col_vals_in_set(HackCols.split, [TEST_SPLIT], brief=f"split is {TEST_SPLIT}")
        .specially(
            vUtils.has_no_hidden_columns,
            brief="student data has no is_leaver or turnover_rate column",
        )
        .col_vals_not_null(
            HackCols.valid_for_analysis, brief="valid_for_analysis has no nulls"
        )
        .interrogate()
    )

    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the student dataset to validate"),
        ("--test_path", "The filepath of the test split dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(
        args.bucket_name,
        args.source_path,
        args.test_path,
        args.reports_path,
    )
    print(f"Validation of {args.source_path} complete")
