import sys

import pointblank as pb
import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.diag_helpers as diag
from polars_utils import utils
from polars_utils.categorical_types import EstablishmentCatType
from projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.categorical_types import (
    JobRoleCatType,
)
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
    """Prototypes a narrow, grain-only load + Categorical + pointblank's stock rows_distinct().

    The previous prototype (Categorical-encoded grain columns, but loading
    all 7 columns of the validated table) still OOM'd, dying somewhere after
    the diag_02_after_categorical_cast checkpoint (~18.1GB peak RSS) inside
    .rows_distinct().interrogate(). Reading pointblank's installed source
    (_interrogation.py:783-804) shows why Categorical-encoding the grain
    columns alone was never going to be enough: RowsDistinct's column-subset
    prep (_utils.py:499-510) does not narrow the table to just the grain
    columns - Interrogator.rows_distinct() joins its group-by count back onto
    `self.x`, the FULL validated table (all 7 columns), not a 3-column grain
    subset. That join reconstructs a full second copy of the entire table
    regardless of grain-column dtype, and RowsDistinct.test() (line 1396)
    converts the resulting boolean column to a Python list - both costs scale
    with row count and total column width, not with how compact the grain
    columns are encoded.

    This prototype instead loads ONLY GRAIN_COLUMNS via
    utils.read_parquet(selected_columns=...) - true column projection at scan
    time, not a post-read .select() - so pointblank's join operates on a
    3-column table instead of a 7-column one, on top of the Categorical
    encoding already confirmed cheap to apply. Matches the narrow-load
    architecture already used elsewhere in this repo (e.g.
    _07_estimate_filled_posts_by_job_role's validate_03_impute.py splits its
    index-uniqueness check into its own narrowly-loaded pb.Validate call).

    Throwaway diagnostic for the ticket 1814 validate_00_prepare OOM - see the
    isolation plan, not part of the permanent pipeline.

    Args:
        bucket_name (str): the bucket containing the source dataset and to
            write diagnostic checkpoints to.
        source_path (str): the filepath of the dataset to read.
        reports_path (str): the filepath to write diagnostic checkpoints to.
    """
    diag.write_checkpoint(bucket_name, reports_path, "diag_02_before_read")

    source_df = utils.read_parquet(
        source=f"s3://{bucket_name}/{source_path}",
        selected_columns=GRAIN_COLUMNS,
    )

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_02_after_narrow_read",
        row_count=source_df.height,
    )

    source_df = source_df.with_columns(
        pl.col(AWPClean.establishment_id).cast(EstablishmentCatType),
        pl.col(AWPJobRoles.job_role_code).cast(JobRoleCatType),
    )
    diag.write_checkpoint(bucket_name, reports_path, "diag_02_after_categorical_cast")

    validation = (
        pb.Validate(data=source_df, label="diag_02_rows_distinct_only")
        .rows_distinct(GRAIN_COLUMNS)
        .interrogate()
    )

    diag.write_checkpoint(
        bucket_name,
        reports_path,
        "diag_02_after_interrogate",
        all_passed=validation.all_passed(),
    )
    print(f"all_passed={validation.all_passed()}", flush=True)


if __name__ == "__main__":
    print(f"Diagnostic script called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--bucket_name", "S3 bucket for source dataset and diagnostic checkpoints"),
        ("--source_path", "The filepath of the dataset to read"),
        ("--reports_path", "The filepath to write diagnostic checkpoints"),
    )

    main(args.bucket_name, args.source_path, args.reports_path)
    print("Diagnostic diag_02_rows_distinct_only complete")
