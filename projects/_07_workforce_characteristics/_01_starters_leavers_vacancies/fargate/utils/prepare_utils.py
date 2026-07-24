import re
from dataclasses import dataclass

import polars as pl
import polars.selectors as cs

from polars_utils import expressions as expr
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

_JOB_ROLE_COLUMN_PATTERN = re.compile(r"^jr0*(\d+)(emp|strt|stop|vacy)$")

_METRIC_ATTR_BY_SUFFIX = {
    "emp": AWPJobRoles.employees,
    "strt": AWPJobRoles.starters,
    "stop": AWPJobRoles.leavers,
    "vacy": AWPJobRoles.vacancies,
}


def reduce_to_published_roles():
    """
    Placeholder function to reduce columns to only published roles plus
    other_dc/other_man etc.
    """
    pass


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
        ValueError: if no columns match `is_slv_job_role_column()`, if a column
            matches it but not the expected `jr<code><metric>` naming pattern, if
            two columns normalise to the same code and metric (e.g. `jr01emp` and
            `jr1emp`), or if a job-role code has some but not all 4 metric
            columns present — these would indicate the SLV column-naming
            convention has changed upstream.
    """
    slv_columns = cs.expand_selector(schema, expr.is_slv_job_role_column())

    if not slv_columns:
        raise ValueError("No SLV job-role columns found in schema.")

    columns_by_code: dict[str, dict[str, str]] = {}
    for column in slv_columns:
        match = _JOB_ROLE_COLUMN_PATTERN.match(column)
        if match is None:
            raise ValueError(
                f"Column '{column}' matched is_slv_job_role_column() but not the "
                "expected jr<code><metric> naming pattern."
            )
        code, suffix = match.group(1), match.group(2)
        metric_attr = _METRIC_ATTR_BY_SUFFIX[suffix]
        code_columns = columns_by_code.setdefault(code, {})
        existing_column = code_columns.get(metric_attr)
        if existing_column is not None:
            raise ValueError(
                f"Columns '{existing_column}' and '{column}' both normalise to "
                f"job role code '{code}' for the same metric."
            )
        code_columns[metric_attr] = column

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

    Metric columns are downcast to Int16 on the struct fields (before
    concat_list/explode), not after — this keeps the intermediate
    list-of-structs column smaller too, not just the final sunk output. Safe
    because upstream bounding already constrains these metrics to [1, 998]
    (see `BoundingExpressions.slv_lower_bound`/`slv_upper_bound`).

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
            `vacancies` metric columns as Int16.
    """
    job_role_structs = [
        pl.struct(
            pl.lit(cols.job_role_code).alias(AWPJobRoles.job_role_code),
            pl.col(cols.employees).cast(pl.Int16).alias(AWPJobRoles.employees),
            pl.col(cols.starters).cast(pl.Int16).alias(AWPJobRoles.starters),
            pl.col(cols.leavers).cast(pl.Int16).alias(AWPJobRoles.leavers),
            pl.col(cols.vacancies).cast(pl.Int16).alias(AWPJobRoles.vacancies),
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
