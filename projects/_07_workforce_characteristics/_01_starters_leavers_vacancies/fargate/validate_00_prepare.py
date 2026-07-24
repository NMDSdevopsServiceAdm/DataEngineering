import sys

import pointblank as pb

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_utils as pUtils
from polars_utils import utils
from polars_utils.validation import actions as vl
from polars_utils.validation.constants import GLOBAL_ACTIONS, GLOBAL_THRESHOLDS
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

COMPARE_COLS_TO_IMPORT = [
    AWPClean.establishment_id,
]

GRAIN_COLUMNS = [
    AWPClean.establishment_id,
    AWPClean.ascwds_workplace_import_date,
    AWPJobRoles.job_role_code,
]

METRIC_COLUMNS = [
    AWPJobRoles.employees,
    AWPJobRoles.starters,
    AWPJobRoles.leavers,
    AWPJobRoles.vacancies,
]


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
    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    # Single scan of compare_path, reused for both the schema-derived job-role
    # count (independent of the output being validated, rather than counting
    # distinct job_role_code values from source_df itself) and the row count.
    compare_lf = utils.scan_parquet(source=f"s3://{bucket_name}/{compare_path}")
    compare_schema = compare_lf.collect_schema()
    job_role_code_count = len(pUtils.discover_job_role_codes(compare_schema))
    compare_row_count = compare_lf.select(COMPARE_COLS_TO_IMPORT).collect().height
    expected_row_count = compare_row_count * job_role_code_count

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
        # index columns
        .rows_distinct(
            GRAIN_COLUMNS,
            brief="Grain should be unique per establishment, import date and job role",
        )
        # complete columns
        .col_vals_not_null(
            columns=GRAIN_COLUMNS,
            brief="Grain columns should contain no null values",
        )
        # numerical
        .col_vals_between(
            columns=METRIC_COLUMNS,
            left=1,
            right=998,
            na_pass=True,
            brief="Metrics should be between 1 and 998 where present.",
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
