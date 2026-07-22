import re
from dataclasses import dataclass

import polars as pl
import polars.selectors as cs

from polars_utils import expressions as expr
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)


def peak_rss_kb() -> int | None:
    """Return peak process RSS in KB, or None on platforms without a `resource` module.

    `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss` is the simplest
    zero-dependency way to sample memory on the Linux Fargate container this
    pipeline runs on. `resource` is POSIX-only, so this returns None on
    developer machines running Windows rather than failing to import.
    """
    try:
        import resource
    except ImportError:
        return None
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


_JOB_ROLE_COLUMN_PATTERN = re.compile(r"^jr0*(\d+)(emp|strt|stop|vacy)$")

_METRIC_ATTR_BY_SUFFIX = {
    "emp": AWPJobRoles.employees,
    "strt": AWPJobRoles.starters,
    "stop": AWPJobRoles.leavers,
    "vacy": AWPJobRoles.vacancies,
}


@dataclass(frozen=True)
class JobRoleCodeColumns:
    """Source column names for one ASC-WDS job-role code's four SLV metrics."""

    job_role_code: str
    employees: str
    starters: str
    leavers: str
    vacancies: str


def discover_job_role_codes(
    schema: pl.Schema | dict[str, pl.DataType],
) -> list[JobRoleCodeColumns]:
    """Discover the ASC-WDS SLV job-role codes present in a schema and their source columns.

    Job-role codes are derived dynamically from the schema (rather than a hardcoded
    list) so this stays robust to ASC-WDS adding or retiring codes over time.

    Args:
        schema (pl.Schema | dict[str, pl.DataType]): schema of the cleaned ASCWDS
            workplace dataset, e.g. from `LazyFrame.collect_schema()`.

    Returns:
        list[JobRoleCodeColumns]: one entry per discovered job-role code, ordered
            by code ascending.

    Raises:
        ValueError: if a column matches `is_slv_job_role_column()` but not the
            expected `jr<code><metric>` naming pattern, or if a job-role code has
            some but not all 4 metric columns present — both would indicate the
            SLV column-naming convention has changed upstream.
    """
    slv_columns = cs.expand_selector(schema, expr.is_slv_job_role_column())

    columns_by_code: dict[str, dict[str, str]] = {}
    for column in slv_columns:
        match = _JOB_ROLE_COLUMN_PATTERN.match(column)
        if match is None:
            raise ValueError(
                f"Column '{column}' matched is_slv_job_role_column() but not the "
                "expected jr<code><metric> naming pattern."
            )
        code, suffix = match.group(1), match.group(2)
        columns_by_code.setdefault(code, {})[_METRIC_ATTR_BY_SUFFIX[suffix]] = column

    incomplete_codes = {
        code: metrics
        for code, metrics in columns_by_code.items()
        if len(metrics) != len(_METRIC_ATTR_BY_SUFFIX)
    }
    if incomplete_codes:
        raise ValueError(
            "Job role code(s) missing one or more of the 4 SLV metric columns: "
            f"{incomplete_codes}"
        )

    return [
        JobRoleCodeColumns(job_role_code=code, **columns_by_code[code])
        for code in sorted(columns_by_code, key=int)
    ]


def convert_job_role_columns_to_rows(
    workplace_lf: pl.LazyFrame,
    index_cols: list[str],
    job_role_columns: list[JobRoleCodeColumns],
) -> pl.LazyFrame:
    """Reshape wide SLV job-role columns into one row per job-role code.

    Builds one struct per job-role code (job_role_code plus its 4 metrics),
    concatenates those structs into a single list column per row, then explodes
    and unnests it into long format. Single pass, no joins.

    Args:
        workplace_lf (pl.LazyFrame): the (already column-pruned) cleaned ASCWDS
            workplace LazyFrame, containing `index_cols` and the SLV job-role
            columns referenced by `job_role_columns`.
        index_cols (list[str]): grain columns preserved through the reshape,
            e.g. establishment_id and ascwds_workplace_import_date.
        job_role_columns (list[JobRoleCodeColumns]): source column names per
            discovered job-role code, from `discover_job_role_codes`.

    Returns:
        pl.LazyFrame: long-format frame with one row per
            (*index_cols, job_role_code), and `employees`/`starters`/`leavers`/
            `vacancies` metric columns.
    """
    job_role_structs = [
        pl.struct(
            pl.lit(cols.job_role_code).alias(AWPJobRoles.job_role_code),
            pl.col(cols.employees).alias(AWPJobRoles.employees),
            pl.col(cols.starters).alias(AWPJobRoles.starters),
            pl.col(cols.leavers).alias(AWPJobRoles.leavers),
            pl.col(cols.vacancies).alias(AWPJobRoles.vacancies),
        )
        for cols in job_role_columns
    ]

    job_roles_lf = (
        workplace_lf.select(
            *index_cols, pl.concat_list(job_role_structs).alias("_job_roles")
        )
        .explode("_job_roles")
        .unnest("_job_roles")
    )
    return job_roles_lf
