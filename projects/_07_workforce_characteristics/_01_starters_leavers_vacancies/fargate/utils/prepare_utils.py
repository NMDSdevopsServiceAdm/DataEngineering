import re
from typing import Generator

import polars as pl
import polars.selectors as cs


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
        _reduce_to_published_roles_expressions(job_role_mapping, job_role_suffixes),
    )

    # Flatten job role lists from job_role_mapping into single list, format them
    # to match column names, then drop those columns.
    old_roles = [old for olds in job_role_mapping.values() for old in olds]
    roles_to_drop = [
        f"jr{role}{suffix}" for role in old_roles for suffix in job_role_suffixes
    ]
    lf = lf.drop(cs.by_name(*roles_to_drop, require_all=False))

    return lf


def _reduce_to_published_roles_expressions(
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


def convert_job_role_strings_to_number_only():
    """
    Placeholder function to 'jr01/02/03' etc into '1/2/3' etc ."""
    pass
