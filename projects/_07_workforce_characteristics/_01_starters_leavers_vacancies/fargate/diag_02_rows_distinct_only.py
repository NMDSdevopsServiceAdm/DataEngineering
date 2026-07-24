import sys

import pointblank as pb

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.diag_helpers as diag
from polars_utils import utils
from polars_utils.validation import actions as vl
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


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Isolates the incremental memory cost of the grain-uniqueness check.

    Runs the same eager read as diag_01_read_parquet_only.py, then only the
    high-cardinality grain-uniqueness check - no row_count_match or the other
    checks. Comparing its peak RSS against diag_01's isolates what the check
    adds on top of the base materialisation.

    Uses has_no_duplicate_grain_rows() rather than pointblank's own
    rows_distinct(), matching the real fix in validate_00_prepare.py - see
    ticket 1814's isolation experiments for why rows_distinct() OOM'd here.

    Records the duplicate count/fraction as its own checkpoint before running
    has_no_duplicate_grain_rows() - a boolean .sum() is a cheap O(n)
    reduction, unlike the .filter() the validator itself does further on, so
    this isolates whether a high duplicate rate (not is_duplicated() itself)
    is what's driving a second OOM here.

    Throwaway diagnostic for the ticket 1814 validate_00_prepare OOM - see the
    isolation plan, not part of the permanent pipeline.

    Args:
        bucket_name (str): the bucket containing the source dataset and to
            write diagnostic checkpoints to.
        source_path (str): the filepath of the dataset to read.
        reports_path (str): the filepath to write diagnostic checkpoints to.
    """
    diag.write_checkpoint(bucket_name, reports_path, "diag_02_before_read")

    source_df = utils.read_parquet(source=f"s3://{bucket_name}/{source_path}")

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_02_after_read",
        row_count=source_df.height,
    )

    is_dup = source_df.select(GRAIN_COLUMNS).is_duplicated()
    dup_count = int(is_dup.sum())
    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_02_after_duplicate_count",
        dup_count=dup_count,
        dup_fraction=dup_count / source_df.height,
    )

    validation = (
        pb.Validate(data=source_df, label="diag_02_rows_distinct_only")
        .specially(
            vl.has_no_duplicate_grain_rows(GRAIN_COLUMNS, bucket_name, reports_path)
        )
        .interrogate()
    )

    diag.write_checkpoint(bucket_name, reports_path, "diag_02_after_interrogate")
    print(validation.get_json_report(), flush=True)


if __name__ == "__main__":
    print(f"Diagnostic script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and diagnostic checkpoints"),
        ("--source_path", "The filepath of the dataset to read"),
        ("--reports_path", "The filepath to write diagnostic checkpoints"),
    )

    main(args.bucket_name, args.source_path, args.reports_path)
    print("Diagnostic diag_02_rows_distinct_only complete")
