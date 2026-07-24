import sys

import pointblank as pb

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.diag_helpers as diag
from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

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


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Isolates the incremental memory cost of the two elementwise checks.

    Runs the same eager read as diag_01_read_parquet_only.py, then only
    col_vals_not_null() and col_vals_between() - no row_count_match or
    rows_distinct(). Both are expected to be cheap, elementwise boolean
    operations rather than group-by/dedup, so this confirms that rather than
    assuming it.

    Throwaway diagnostic for the ticket 1814 validate_00_prepare OOM - see the
    isolation plan, not part of the permanent pipeline.

    Args:
        bucket_name (str): the bucket containing the source dataset and to
            write diagnostic checkpoints to.
        source_path (str): the filepath of the dataset to read.
        reports_path (str): the filepath to write diagnostic checkpoints to.
    """
    diag.write_checkpoint(bucket_name, reports_path, "diag_03_before_read")

    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_03_after_read",
        row_count=source_df.height,
    )

    validation = (
        pb.Validate(data=source_df, label="diag_03_cheap_checks_only")
        .col_vals_not_null(columns=GRAIN_COLUMNS)
        .col_vals_between(columns=METRIC_COLUMNS, left=1, right=998, na_pass=True)
        .interrogate()
    )

    diag.write_checkpoint(bucket_name, reports_path, "diag_03_after_interrogate")
    print(validation.get_json_report(), flush=True)


if __name__ == "__main__":
    print(f"Diagnostic script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and diagnostic checkpoints"),
        ("--source_path", "The filepath of the dataset to read"),
        ("--reports_path", "The filepath to write diagnostic checkpoints"),
    )

    main(args.bucket_name, args.source_path, args.reports_path)
    print("Diagnostic diag_03_cheap_checks_only complete")
