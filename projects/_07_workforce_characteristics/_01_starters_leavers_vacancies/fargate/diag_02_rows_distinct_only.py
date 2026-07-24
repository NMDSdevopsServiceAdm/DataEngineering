import sys

import polars as pl

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

MAX_ROWS_TO_EXTRACT = 1000


def main(bucket_name: str, source_path: str, reports_path: str) -> None:
    """Prototypes a group_by()+semi-join replacement for is_duplicated().

    is_duplicated() was confirmed to be the OOM culprit, not the .filter()
    extraction step originally suspected: diag_02_after_read now succeeds at
    ~15.4GB peak RSS (matching diag_01), but diag_02_after_duplicate_count
    (computed via source_df.select(GRAIN_COLUMNS).is_duplicated()) never
    appears. This bypasses has_no_duplicate_grain_rows() entirely for this
    diagnostic and instead prototypes SPEC.md's already-documented "Option 2"
    fallback - group_by()+len() for detection, a how="semi" join (bounded to
    MAX_ROWS_TO_EXTRACT duplicate-group keys) for the extract - to confirm it
    avoids the OOM before changing the real validator.

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

    dup_groups = (
        source_df.select(GRAIN_COLUMNS)
        .group_by(GRAIN_COLUMNS)
        .len()
        .filter(pl.col("len") > 1)
    )
    dup_group_count = dup_groups.height
    has_duplicates = dup_group_count > 0
    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_02_after_group_by_check",
        dup_group_count=dup_group_count,
        has_duplicates=has_duplicates,
    )

    if has_duplicates:
        dup_rows = source_df.join(
            dup_groups.select(GRAIN_COLUMNS).head(MAX_ROWS_TO_EXTRACT),
            on=GRAIN_COLUMNS,
            how="semi",
        ).head(MAX_ROWS_TO_EXTRACT)
        diag.write_checkpoint(
            bucket_name,
            reports_path,
            "diag_02_after_semi_join_extract",
            extracted_row_count=dup_rows.height,
        )

    diag.write_checkpoint(bucket_name, reports_path, "diag_02_complete")
    print(
        f"has_duplicates={has_duplicates}, dup_group_count={dup_group_count}",
        flush=True,
    )


if __name__ == "__main__":
    print(f"Diagnostic script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and diagnostic checkpoints"),
        ("--source_path", "The filepath of the dataset to read"),
        ("--reports_path", "The filepath to write diagnostic checkpoints"),
    )

    main(args.bucket_name, args.source_path, args.reports_path)
    print("Diagnostic diag_02_rows_distinct_only complete")
