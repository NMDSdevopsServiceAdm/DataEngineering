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
    """Prototypes Categorical-encoded grain columns + pointblank's stock rows_distinct().

    Both is_duplicated() and a group_by()+semi-join replacement (this
    script's two previous prototypes) were confirmed to OOM at production
    scale regardless of algorithm - the group_by version OOM'd before even
    reaching its own diag_02_after_group_by_check checkpoint, isolating the
    real cost to hashing/grouping ~370M rows of String-typed grain columns,
    not to any particular join/no-join design.

    This prototype instead Categorical-encodes establishment_id/job_role_code
    (matching the cast now applied upstream in _00_prepare.py/prepare_utils.py)
    and reverts to pointblank's plain built-in .rows_distinct(), to confirm
    that's what actually makes grain-uniqueness checking viable at this row
    count, before applying the same change to the real validator.

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
