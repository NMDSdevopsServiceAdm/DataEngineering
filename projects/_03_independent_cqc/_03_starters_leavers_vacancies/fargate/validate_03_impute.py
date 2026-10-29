import sys

import pointblank as pb
import polars as pl

from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)

COMPARE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
]

# Only what the checks need: the whole impute output is wide.
SOURCE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
    IndCqcColumns.published_job_role_label,
    SLVCols.ascwds_turnover_rate,
    SLVCols.ascwds_turnover_rate_imputed,
]

KNOWN_RATE_KEPT_BRIEF = "imputed turnover rate equals the known rate where known"
IMPUTED_NOT_NEGATIVE_BRIEF = "imputed turnover rate is not negative"
IMPUTED_POPULATED_BRIEF = "imputed turnover rate is populated for every location and job role with a known rate"


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
        # imputed turnover rate
        .col_vals_ge(
            SLVCols.ascwds_turnover_rate_imputed,
            0,
            na_pass=True,
            brief=IMPUTED_NOT_NEGATIVE_BRIEF,
        )
        .col_vals_expr(
            pl.col(SLVCols.ascwds_turnover_rate).is_null()
            | pl.col(SLVCols.ascwds_turnover_rate).eq_missing(
                pl.col(SLVCols.ascwds_turnover_rate_imputed)
            ),
            brief=KNOWN_RATE_KEPT_BRIEF,
        )
        .col_vals_expr(
            pl.col(SLVCols.ascwds_turnover_rate_imputed).is_not_null()
            | ~pl.col(SLVCols.ascwds_turnover_rate)
            .is_not_null()
            .any()
            .over(IndCqcColumns.location_id, IndCqcColumns.published_job_role_label),
            brief=IMPUTED_POPULATED_BRIEF,
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
