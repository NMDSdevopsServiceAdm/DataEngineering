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
from utils.column_values.categorical_column_values import EmploymentStatusFilteringRule
from utils.column_values.categorical_columns_by_dataset import (
    EmploymentStatusCleanCategoricalValues as CatValues,
)

COMPARE_COLS_TO_IMPORT = [
    IndCqcColumns.location_id,
]

DEDUPLICATED_COUNT_COLUMNS = [
    EmpStatus.permanent_count_dedup,
    EmpStatus.temporary_count_dedup,
    EmpStatus.bank_or_pool_count_dedup,
    EmpStatus.agency_count_dedup,
    EmpStatus.other_count_dedup,
]

PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage,
    EmpStatus.temporary_percentage,
    EmpStatus.bank_or_pool_percentage,
    EmpStatus.agency_percentage,
    EmpStatus.other_percentage,
]

CLEAN_COUNT_COLUMNS = [
    EmpStatus.permanent_count_clean,
    EmpStatus.temporary_count_clean,
    EmpStatus.bank_or_pool_count_clean,
    EmpStatus.agency_count_clean,
    EmpStatus.other_count_clean,
]

CLEAN_PERCENTAGE_COLUMNS = [
    EmpStatus.permanent_percentage_clean,
    EmpStatus.temporary_percentage_clean,
    EmpStatus.bank_or_pool_percentage_clean,
    EmpStatus.agency_percentage_clean,
    EmpStatus.other_percentage_clean,
]


def _clean_count_matches_filtering_rule_expr(column: str) -> pl.Expr:
    """Builds the "column is null iff filtering_rule isn't 'populated'" check.

    Only holds exactly for the _clean count columns, which are derived purely
    from the raw counts and this stage's own ratio rules.
    """
    populated = pl.lit(EmploymentStatusFilteringRule.populated)
    return (
        (pl.col(EmpStatus.filtering_rule) != populated) & pl.col(column).is_null()
    ) | ((pl.col(EmpStatus.filtering_rule) == populated) & pl.col(column).is_not_null())


def _clean_percentage_is_null_when_not_populated_expr(column: str) -> pl.Expr:
    """Builds the one-directional "column is null when filtering_rule isn't
    'populated'" check.

    Doesn't require non-null when populated: _clean percentage columns can
    also be null because their source _percentage was already null (an
    unrelated dedup-staleness reason from create_employment_status_percentage_
    columns), not just because this stage's ratio rules nulled them.
    """
    populated = pl.lit(EmploymentStatusFilteringRule.populated)
    return (pl.col(EmpStatus.filtering_rule) == populated) | pl.col(column).is_null()


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
        exclude_complex_types=True,
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
        .col_vals_ge(
            DEDUPLICATED_COUNT_COLUMNS,
            0,
            na_pass=True,
            brief="deduplicated employment status counts are greater than or equal to 0",
        )
        .col_vals_between(
            [*PERCENTAGE_COLUMNS, *CLEAN_PERCENTAGE_COLUMNS],
            0,
            1,
            na_pass=True,
            brief="employment status percentages are between 0 and 1",
        )
        # complete columns
        .col_vals_not_null(
            [
                EmpStatus.filtering_rule,
            ]
        )
        # categorical
        .col_vals_in_set(
            EmpStatus.filtering_rule,
            CatValues.filtering_rule_column_values.categorical_values,
            brief="employment_status_filtering_rule is a known reason",
        )
    )
    for column in CLEAN_COUNT_COLUMNS:
        validation = validation.col_vals_expr(
            expr=_clean_count_matches_filtering_rule_expr(column),
            brief=f"{column} must be null when {EmpStatus.filtering_rule} isn't 'populated', and non-null when it is",
        )
    for column in CLEAN_PERCENTAGE_COLUMNS:
        validation = validation.col_vals_expr(
            expr=_clean_percentage_is_null_when_not_populated_expr(column),
            brief=f"{column} must be null when {EmpStatus.filtering_rule} isn't 'populated'",
        )
    validation = validation.interrogate()
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
