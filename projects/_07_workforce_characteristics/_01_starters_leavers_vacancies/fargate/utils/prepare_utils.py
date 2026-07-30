import re

import polars as pl

from polars_utils.categorical_types import EstablishmentCatType, JobRoleCatType
from polars_utils.expressions import is_slv_job_role_column
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SlvJobRoleColumns as SLVJR

JOB_ROLE_CODE_PATTERN = re.compile(r"^jr(\d+)(?:emp|strt|stop|vacy)$")
SUFFIX_TO_METRIC_COLUMN = {
    "emp": SLVJR.employees,
    "strt": SLVJR.starters,
    "stop": SLVJR.leavers,
    "vacy": SLVJR.vacancies,
}

# "Total, all roles" (28) and "total, job group" (29-32) summary columns -
# these aren't real job roles, so _00_prepare.py drops them before pivoting.
JOB_ROLE_SUMMARY_COLUMNS_PATTERN = r"^jr(28|29|30|31|32)(emp|strt|stop|vacy)$"

unpublished_roles_mapping = {
    "1001": ["02", "03", "05", "24", "45", "47", "49", "50"], # other managers
    "1002": ["35", "37"], # other regulated professions
    "1003": ["10", "11", "23", "38"], # other direct care
    "1004": ["25", "26", "27", "34", "36", "39", "40", "42", "44", "46", "48", "51"], # other
} # fmt: skip


def discover_job_role_codes(schema: pl.Schema | dict[str, pl.DataType]) -> list[str]:
    """
    Discovers the distinct ASC-WDS job-role codes present in a wide schema.

    Shared by `pivot_job_role_cols_to_rows()` and `discover_job_role_code_count()`
    so their discovery logic can't drift apart. Returns an empty list rather
    than raising when no job-role columns are found, since callers disagree on
    whether that's an error.

    Args:
        schema (pl.Schema | dict[str, pl.DataType]): Schema of a wide ASC-WDS
            workplace dataset with `jrNN{emp,strt,stop,vacy}` job-role columns.

    Returns:
        list[str]: Sorted, distinct job-role codes (e.g. "01", "1001"), or
            empty if none are found.

    Raises:
        ValueError: If a selected column doesn't match the expected
            `jrNN{suffix}` naming pattern.
    """
    job_role_columns = (
        pl.LazyFrame(schema=schema)
        .select(is_slv_job_role_column())
        .collect_schema()
        .names()
    )

    codes = set()
    for col in job_role_columns:
        match = JOB_ROLE_CODE_PATTERN.match(col)
        if match is None:
            raise ValueError(
                f"Column '{col}' matched is_slv_job_role_column() but not the "
                "expected jrNN{suffix} pattern."
            )
        codes.add(match.group(1))

    return sorted(codes)


def pivot_job_role_cols_to_rows(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Reshapes the wide ASC-WDS job-role columns into one row per job role.

    Builds one struct per job-role code ({job_role_code, employees, starters,
    leavers, vacancies}), concatenates them into a list column, then
    explodes/unnests into rows - a join-free technique (a join-based
    alternative OOM'd at comparable scale). Only grain and job-role metric
    columns are kept; other columns are dropped rather than broadcast onto
    every exploded row, which would multiply their memory cost by the
    job-role count.

    Args:
        lf (pl.LazyFrame): Cleaned ASC-WDS workplace LazyFrame with wide
            `jrNN{emp,strt,stop,vacy}` job-role columns.

    Returns:
        pl.LazyFrame: One row per (establishment_id,
            ascwds_workplace_import_date, job_role_code), with
            employees/starters/leavers/vacancies columns. Rows where all four
            metrics are null are kept, not dropped.

    Raises:
        ValueError: If no job-role columns are found, or if a discovered
            column doesn't match the expected pattern - see
            `discover_job_role_codes()`.
    """
    job_role_codes = discover_job_role_codes(lf.collect_schema())

    if not job_role_codes:
        raise ValueError("No job role columns found to pivot.")

    job_role_structs = [
        pl.struct(
            pl.lit(str(int(code))).alias(SLVJR.job_role_code),
            *[
                pl.col(f"jr{code}{suffix}").alias(metric_column)
                for suffix, metric_column in SUFFIX_TO_METRIC_COLUMN.items()
            ],
        )
        for code in job_role_codes
    ]

    lf = lf.select(
        AWPClean.establishment_id,
        AWPClean.ascwds_workplace_import_date,
        pl.concat_list(job_role_structs).alias("job_roles"),
    )
    exploded_lf = lf.explode("job_roles").unnest("job_roles")

    # Int16 (~32,767 max) comfortably covers realistic per-establishment,
    # per-job-role employee/starter/leaver/vacancy counts; strict=False guards
    # against an unexpected outlier nulling that one metric rather than
    # failing the whole pipeline run.
    exploded_lf = exploded_lf.with_columns(
        pl.col(AWPClean.establishment_id).cast(EstablishmentCatType),
        pl.col(SLVJR.job_role_code).cast(JobRoleCatType),
        pl.col(SLVJR.employees).cast(pl.Int16, strict=False),
        pl.col(SLVJR.starters).cast(pl.Int16, strict=False),
        pl.col(SLVJR.leavers).cast(pl.Int16, strict=False),
        pl.col(SLVJR.vacancies).cast(pl.Int16, strict=False),
    )

    return exploded_lf
