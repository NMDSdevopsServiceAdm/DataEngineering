import re
from typing import Generator

import polars as pl
import polars.selectors as cs

from utils.column_values.categorical_column_values import PublishedJobRoleLabels
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

JOB_ROLE_COLUMN_PATTERN = re.compile(r"^jr(\d+)([a-z]+)$")

SYNTHETIC_JOB_ROLE_LABELS = {
    "1001": PublishedJobRoleLabels.other_managers,
    "1002": PublishedJobRoleLabels.other_regulated_professions,
    "1003": PublishedJobRoleLabels.other_direct_care,
    "1004": PublishedJobRoleLabels.other,
}


def reduce_to_published_roles(
    lf: pl.LazyFrame, job_role_mapping: dict[str, list[str]]
) -> pl.LazyFrame:
    """
    Merge ASC-WDS workplace job role columns down to published roles.

    For each key job role code in job_role_mapping, sums the key job role
    together with all the listed job role columns. This sum then replaces the
    key job roles value. The listed job role columns are then dropped, leaving
    only published roles plus the 'other' groups (other_dc/other_man etc.).

    Args:
        lf (pl.LazyFrame): ASC-WDS workplace LazyFrame.
        job_role_mapping (dict[str, list[str]]): A mapping of job roles.
            E.g. {role_to_merge_and_keep: [role_1_to_merge_and_drop, role_2_to_merge_and_drop...]}

    Returns:
        pl.LazyFrame: Input LazyFrame in which columns have been merged and
            removed.
    """
    job_role_cols = lf.collect_schema().names()
    job_role_suffixes = list(
        {re.sub(r"^jr\d+", "", col) for col in job_role_cols if col.startswith("jr")}
    )

    lf = lf.with_columns(
        reduce_to_published_roles_expressions(job_role_mapping, job_role_suffixes),
    )

    # Flatten job role lists from job_role_mapping into single list, format them
    # to match column names, then drop those columns.
    old_roles = [old for olds in job_role_mapping.values() for old in olds]
    roles_to_drop = [
        f"jr{role}{suffix}" for role in old_roles for suffix in job_role_suffixes
    ]
    lf = lf.drop(cs.by_name(*roles_to_drop, require_all=False))

    return lf


def reduce_to_published_roles_expressions(
    job_role_mapping: dict[str, list[str]], slv_suffixes: list[str]
) -> Generator[pl.Expr, None, None]:
    """
    A generator function that yields Polars expressions that sum
    ASC-WDS workplace job role columns in the given mapping dictionary
    that have the given slv_suffixes.

    When all columns to sum are null then expression produces null.

    Args:
        job_role_mapping (dict[str, list[str]]): A mapping of job roles.
            E.g. {role_to_merge_and_keep: [role_1_to_merge_and_drop, role_2_to_merge_and_drop...]}
        slv_suffixes (list[str]): A list of ASC-WDS workplace job role column suffixes.
            E.g. ["flag", "emp", "work"]

    Yields:
        pl.Expr: Polars expressions for summing columns.

    """
    for role_to_keep, roles_to_merge in job_role_mapping.items():
        for suffix in slv_suffixes:
            prefixes = [f"jr{role_to_keep}"] + [f"jr{old}" for old in roles_to_merge]
            cols = cs.starts_with(*prefixes) & cs.ends_with(suffix)
            yield (
                pl.when(pl.all_horizontal(cols.is_null()))
                .then(pl.lit(None))
                .otherwise(pl.sum_horizontal(cols))
                .alias(f"jr{role_to_keep}{suffix}")
            )


def pivot_job_role_cols_to_rows():
    """
    Placeholder function to pivot job role columns into rows to create column
    for job role number and columns for emps, starters, leavers and vacancies.
    """
    pass


def relabel_job_role_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Renames jrNN{suffix} columns to their published job role label.

    Suffix-agnostic: each column's suffix is derived from its own name
    (whatever follows the numeric code) rather than a fixed suffix list, so
    it keeps working if new suffixes are introduced upstream. Real ASC-WDS
    codes are looked up in the raw-data mapping
    (AscwdsWorkerValueLabelsMainjrid.labels_dict); the four synthetic codes
    produced by reduce_to_published_roles are looked up in
    SYNTHETIC_JOB_ROLE_LABELS instead. Columns not matching the jrNN{suffix}
    shape are left untouched.

    Args:
        lf (pl.LazyFrame): LazyFrame with jrNN{suffix} columns reduced to
            published roles, before any pivot.

    Returns:
        pl.LazyFrame: Input LazyFrame with job role columns renamed to
            {published_label}_{suffix}.

    Raises:
        ValueError: if a jrNN{suffix} column's code has no corresponding
            label - the mapping is expected to be exhaustive by this stage.
    """
    rename_mapping = {}
    for col in lf.collect_schema().names():
        match = JOB_ROLE_COLUMN_PATTERN.match(col)
        if not match:
            continue
        code, suffix = match.groups()

        if code in SYNTHETIC_JOB_ROLE_LABELS:
            label = SYNTHETIC_JOB_ROLE_LABELS[code]
        else:
            stripped_code = code.lstrip("0")
            if stripped_code not in AscwdsWorkerValueLabelsMainjrid.labels_dict:
                raise ValueError(f"Unmapped job role code: '{code}'")
            label = AscwdsWorkerValueLabelsMainjrid.labels_dict[stripped_code]

        rename_mapping[col] = f"{label}_{suffix}"

    return lf.rename(rename_mapping)
