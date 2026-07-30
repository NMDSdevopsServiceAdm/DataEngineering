import sys

import pointblank as pb
import polars as pl
import polars.selectors as cs

import polars_utils.cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.filtering_utils import (
    earliest_file_per_month_filter_expr,
    reduced_data_filter_expr,
)
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils import (
    JOB_ROLE_SUMMARY_COLUMNS_PATTERN,
    discover_job_role_codes,
    unpublished_roles_mapping,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SlvJobRoleColumns as SLVJR

COMPARE_COLS_TO_IMPORT = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
]

GRAIN_COLUMNS = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
    SLVJR.job_role_code,
]


def discover_job_role_code_count(schema: pl.Schema | dict[str, pl.DataType]) -> int:
    """
    Counts the distinct ASC-WDS job-role codes present in a wide schema.

    Thin wrapper around `prepare_utils.discover_job_role_codes()`, so the
    expected row-count multiplier here can't drift out of step with the
    reshape's own discovery logic.

    Args:
        schema (pl.Schema | dict[str, pl.DataType]): Schema of the wide,
            pre-reshape ASC-WDS workplace dataset.

    Returns:
        int: Count of distinct job-role codes discovered.
    """
    return len(discover_job_role_codes(schema))


def discover_job_role_code_count_after_transforms(
    raw_schema: pl.Schema | dict[str, pl.DataType],
) -> int:
    """
    Counts the distinct job-role codes _00_prepare.py's pivot actually sees.

    _00_prepare.py::main() merges unpublished job-role codes into synthetic
    1001-1004 codes and drops the jr28-32 summary columns before pivoting, so
    counting against the raw schema would overcount codes that never reach
    the pivot. This replicates both transforms on a zero-row LazyFrame
    (schema-only, so safe with no rows) before counting. Confirmed in
    production: 52 raw job-role columns collapse to 25 after this step.

    Args:
        raw_schema (pl.Schema | dict[str, pl.DataType]): Schema of the wide, raw
            (pre-merge, pre-drop) ASC-WDS workplace dataset, as returned by
            discover_combined_schema() against the compare dataset.

    Returns:
        int: Count of distinct job-role codes remaining after replicating
            _00_prepare.py's merge_job_role_columns() and jr28-32 drop.
    """
    lf = pl.LazyFrame(schema=raw_schema)
    lf = cUtils.merge_job_role_columns(lf, unpublished_roles_mapping)
    lf = lf.drop(cs.matches(JOB_ROLE_SUMMARY_COLUMNS_PATTERN))
    return discover_job_role_code_count(lf.collect_schema())


def main(
    bucket_name: str, source_path: str, compare_path: str, reports_path: str
) -> None:
    """Validates a dataset and produces a summary report plus failure outputs.

    The compare dataset is the unreduced cleaned ASCWDS workplace data, so the
    same reduction filters used in _00_prepare are applied here before
    counting rows, otherwise the expected count would include the historical
    rows and duplicate monthly files the prepare step deliberately drops. The
    expected count is then multiplied by the job-role code count from
    discover_job_role_code_count_after_transforms() - not the raw compare
    schema directly - since _00_prepare reshapes one row per
    establishment/date into one row per (establishment, date, job role), and
    counting against the raw schema would overcount codes that never reach
    the pivot.

    Args:
        bucket_name (str): the bucket (name only) in which to source the dataset
            and output the report to (should correspond to workspace / feature
            branch name)
        source_path (str): the source dataset path to be validated
        compare_path (str): the path to the dataset to compare against
        reports_path (str): the output path to write reports to
    """
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    # The union schema (not just the first file's) is used because job-role codes
    # can be added/dropped over time, and a schema based on one file could silently
    # under/over-count them.
    compare_schema = utils.discover_combined_schema(
        f"s3://{bucket_name}/{compare_path}"
    )
    job_role_code_count = discover_job_role_code_count_after_transforms(compare_schema)

    compare_df = (
        utils.read_parquet(
            source=f"s3://{bucket_name}/{compare_path}",
            schema=compare_schema,
            selected_columns=COMPARE_COLS_TO_IMPORT,
        )
        .filter(
            reduced_data_filter_expr(date_col=AWPClean.ascwds_workplace_import_date)
        )
        .filter(
            earliest_file_per_month_filter_expr(
                date_col=AWPClean.ascwds_workplace_import_date
            )
        )
    )
    expected_row_count = compare_df.height * job_role_code_count

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
        # grain uniqueness
        .rows_distinct(
            columns_subset=GRAIN_COLUMNS,
            brief="Primary key (establishment_id, ascwds_workplace_import_date, job_role_code) should be unique",
        ).interrogate()
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
