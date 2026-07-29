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


def pivot_job_role_cols_to_rows(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Reshapes the wide ASC-WDS job-role columns into one row per job role.

    For each job-role code discovered via `is_slv_job_role_column()`, builds a
    struct of {job_role_code, employees, starters, leavers, vacancies}, then
    concatenates one struct per code into a list column and explodes/unnests
    it into rows - the "Candidate B" technique, proven at production scale
    with no joins involved (a join-based alternative OOM'd immediately at
    comparable scale). The output narrows to grain plus job-role metric
    columns only; the other cleaned-workplace columns are dropped rather than
    broadcast onto the exploded rows, which would multiply their memory cost
    by the job-role count.

    Args:
        lf (pl.LazyFrame): Cleaned ASC-WDS workplace LazyFrame with wide
            `jrNN{emp,strt,stop,vacy}` job-role columns.

    Returns:
        pl.LazyFrame: One row per (establishment_id,
            ascwds_workplace_import_date, job_role_code), with
            employees/starters/leavers/vacancies metric columns. Rows where
            all four metrics are null are kept, not dropped.

    Raises:
        ValueError: If no job-role columns are found, indicating an upstream
            schema regression.
    """
    job_role_columns = lf.select(is_slv_job_role_column()).collect_schema().names()

    if not job_role_columns:
        raise ValueError("No job role columns found to pivot.")

    job_role_codes = sorted(
        {JOB_ROLE_CODE_PATTERN.match(col).group(1) for col in job_role_columns}
    )

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
    lf = lf.explode("job_roles").unnest("job_roles")

    lf = lf.with_columns(
        pl.col(AWPClean.establishment_id).cast(EstablishmentCatType),
        pl.col(SLVJR.job_role_code).cast(JobRoleCatType),
        pl.col(SLVJR.employees).cast(pl.Int16, strict=False),
        pl.col(SLVJR.starters).cast(pl.Int16, strict=False),
        pl.col(SLVJR.leavers).cast(pl.Int16, strict=False),
        pl.col(SLVJR.vacancies).cast(pl.Int16, strict=False),
    )

    return lf
