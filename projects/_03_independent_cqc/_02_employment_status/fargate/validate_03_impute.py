import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns

COMPARE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
]

IMPUTED_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_imputed,
    EmpStatus.temporary_percentage_imputed,
    EmpStatus.bank_or_pool_percentage_imputed,
    EmpStatus.agency_percentage_imputed,
    EmpStatus.other_percentage_imputed,
]

ROLLING_AVERAGE_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_rolling_avg,
    EmpStatus.temporary_percentage_rolling_avg,
    EmpStatus.bank_or_pool_percentage_rolling_avg,
    EmpStatus.agency_percentage_rolling_avg,
    EmpStatus.other_percentage_rolling_avg,
]

# Only what the checks need: the whole impute output is wide and includes list columns,
# which pointblank can't write out when it extracts failing rows.
SOURCE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
    IndCqcColumns.published_job_role_label,
    IndCqcColumns.cqc_location_import_date,
    *IMPUTED_PERCENTAGE_COLUMNS,
    *ROLLING_AVERAGE_PERCENTAGE_COLUMNS,
]

# Imputed percentages are Float32, so their sum can drift slightly from 1.
PERCENTAGE_SUM_TOLERANCE = 1e-4


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset according to a set of provided rules and produces a
        summary report as well as failure outputs.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=SOURCE_COLS_TO_IMPORT,
    )
    compare_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{compare_path}",
        selected_columns=COMPARE_COLS_TO_IMPORT,
    )
    expected_row_count = compare_df.height

    validation = (
        pb.Validate(
            data=source_df,
            label=f"Validation of {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        # dataset size
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        # imputed and rolling average percentages
        .col_vals_between(
            IMPUTED_PERCENTAGE_COLUMNS,
            0,
            1,
            na_pass=True,
            brief="imputed percentage columns are between 0 and 1",
        )
        .col_vals_between(
            ROLLING_AVERAGE_PERCENTAGE_COLUMNS,
            0,
            1,
            na_pass=True,
            brief="rolling average percentage columns are between 0 and 1",
        )
        .col_vals_expr(
            pl.col(EmpStatus.permanent_percentage_imputed).is_null()
            | (
                (pl.sum_horizontal(IMPUTED_PERCENTAGE_COLUMNS) - 1).abs()
                <= PERCENTAGE_SUM_TOLERANCE
            ),
            brief="imputed percentages sum to 1 where populated",
        )
        .interrogate()
    )
    vl.write_reports(validation, bucket_name, reports_path)


if __name__ == "__main__":
    print(f"Validation script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and validation report"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to output reports"),
    )
    print(f"Starting validation for {args.source_path}")

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print(f"Validation of {args.source_path} complete")
