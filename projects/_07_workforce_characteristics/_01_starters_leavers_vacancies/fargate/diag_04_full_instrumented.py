import sys

import pointblank as pb

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.diag_helpers as diag
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
    """Runs validate_00_prepare.py's full logic with peak-RSS checkpoints at each stage.

    Sanity-checks whether diag_01/02/03's isolated costs add up to the full
    observed OOM, and is the only one of the diagnostics that can catch
    anything the isolated experiments miss (e.g. pointblank accumulating
    internal copies across chained checks, or a genuine validation failure
    making write_reports()'s get_data_extracts() pull an unexpectedly large
    failing-row set).

    Throwaway diagnostic for the ticket 1814 validate_00_prepare OOM - see the
    isolation plan, not part of the permanent pipeline.

    Args:
        bucket_name (str): the bucket containing the source/compare datasets
            and to write diagnostic checkpoints and reports to.
        source_path (str): the source dataset path to be validated.
        compare_path (str): the path to the dataset to compare against.
        reports_path (str): the output path to write diagnostic checkpoints
            and reports to.
    """
    diag.write_checkpoint(bucket_name, reports_path, "diag_04_before_read")

    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_04_after_read",
        row_count=source_df.height,
    )

    compare_lf = utils.scan_parquet(source=f"s3://{bucket_name}/{compare_path}")
    compare_schema = compare_lf.collect_schema()
    job_role_code_count = len(pUtils.discover_job_role_codes(compare_schema))
    compare_row_count = compare_lf.select(COMPARE_COLS_TO_IMPORT).collect().height
    expected_row_count = compare_row_count * job_role_code_count

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_04_after_expected_row_count",
        expected_row_count=expected_row_count,
    )

    validation = (
        pb.Validate(
            data=source_df,
            label=f"diag_04_full_instrumented {source_path}",
            thresholds=GLOBAL_THRESHOLDS,
            brief=True,
            actions=GLOBAL_ACTIONS,
        )
        .row_count_match(
            expected_row_count,
            brief=f"Expects {expected_row_count} rows",
        )
        .rows_distinct(
            GRAIN_COLUMNS,
            brief="Grain should be unique per establishment, import date and job role",
        )
        .col_vals_not_null(
            columns=GRAIN_COLUMNS,
            brief="Grain columns should contain no null values",
        )
        .col_vals_between(
            columns=METRIC_COLUMNS,
            left=1,
            right=998,
            na_pass=True,
            brief="Metrics should be between 1 and 998 where present.",
        )
        .interrogate()
    )

    diag.write_checkpoint(bucket_name, reports_path, "diag_04_after_interrogate")

    vl.write_reports(validation, bucket_name, reports_path)

    diag.write_checkpoint(bucket_name, reports_path, "diag_04_after_write_reports")


if __name__ == "__main__":
    print(f"Diagnostic script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and diagnostic checkpoints"),
        ("--source_path", "The filepath of the dataset to validate"),
        ("--compare_path", "The filepath of the dataset to compare against"),
        ("--reports_path", "The filepath to write diagnostic checkpoints and reports"),
    )

    main(args.bucket_name, args.source_path, args.compare_path, args.reports_path)
    print("Diagnostic diag_04_full_instrumented complete")
