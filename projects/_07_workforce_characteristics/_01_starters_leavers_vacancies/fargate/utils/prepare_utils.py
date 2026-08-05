import re
from typing import Generator

import polars as pl
import polars.selectors as cs

from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    MainJobRoleID,
    MainJobRoleLabels,
    PublishedJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)


def _codes(column_values_cls) -> dict[str, str]:
    """Return {field_name: code} for a ColumnValues subclass's own fields.

    `vars()` on a dataclass returns only that class's own attributes, not
    inherited ones, so this naturally excludes ColumnValues' base fields
    (column_name, value_to_remove, contains_null_values) without needing to
    name them.
    """
    return {
        name: value
        for name, value in vars(column_values_cls).items()
        if not name.startswith("_")
    }


_role_id_by_name = _codes(MainJobRoleID)
_role_label_by_name = _codes(MainJobRoleLabels)

# PublishedJobRoleLabels' 4 synthetic other_* fields have no MainJobRoleID
# equivalent, so intersecting drops them without needing to name them either.
_published_names = set(_codes(PublishedJobRoleLabels)) & set(_role_id_by_name)

# Every raw ASC-WDS job role code this team has catalogued, zero-padded to
# match real column names (MainJobRoleID stores bare codes, e.g. "1", not "01").
_ALL_CATALOGUED_JOB_ROLE_CODES = {code.zfill(2) for code in _role_id_by_name.values()}

# Published roles are left untouched by reduce_to_published_roles.
PUBLISHED_JOB_ROLE_CODES = {
    _role_id_by_name[name].zfill(2) for name in _published_names
}

# Job group -> synthetic "other_*" job role code it merges into. These 4 codes
# are not real ASC-WDS codes. NOTE: ticket 1794 (separate, unmerged branch)
# independently introduces its own reference to these same 4 codes for a later
# column-relabelling step - this is deliberate, accepted duplication, not to be
# unified here.
_other_role_code_by_job_group = {
    JobGroupLabels.managers: "1001",
    JobGroupLabels.regulated_professions: "1002",
    JobGroupLabels.direct_care: "1003",
    JobGroupLabels.other: "1004",
}

# {other_role_code: [unpublished_raw_codes]}, the shape
# reduce_to_published_roles_expressions sums over. Built from every
# MainJobRoleLabels role that isn't published, bucketed via its job group.
# `technician` (code 22) and `care_navigator` (code 41) exist in MainJobRoleID
# but have no MainJobRoleLabels entry, so this loop never sees them - they're
# already merged upstream into other codes by clean_ascwds_workplace.py's
# legacy_job_roles_dict during ASCWDS ingest cleaning, before this stage runs.
OTHER_ROLE_CODE_TO_UNPUBLISHED_JOB_ROLE_CODES: dict[str, list[str]] = {}
for _name, _label in sorted(_role_label_by_name.items()):
    if _name in _published_names:
        continue
    _job_group = AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict[_label]
    _other_code = _other_role_code_by_job_group[_job_group]
    OTHER_ROLE_CODE_TO_UNPUBLISHED_JOB_ROLE_CODES.setdefault(_other_code, []).append(
        _role_id_by_name[_name].zfill(2)
    )


def reduce_to_published_roles(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merge ASC-WDS workplace job role columns down to published roles.

    Published job roles (PublishedJobRoleLabels) are left untouched. Every
    other catalogued raw job role (MainJobRoleID) is summed into whichever of
    the 4 'other_*' synthetic roles (jr1001/jr1002/jr1003/jr1004) its job group
    (AscwdsWorkerValueLabelsJobGroup) maps to, then dropped. A sum is null only
    when all of its contributing columns are null. Raises ValueError (via
    `_raise_if_uncatalogued_job_role_codes`) if a job role column's code isn't
    in MainJobRoleID.

    Args:
        lf (pl.LazyFrame): ASC-WDS workplace LazyFrame.

    Returns:
        pl.LazyFrame: Input LazyFrame in which unpublished job role columns
            have been merged into the other_* roles and removed.
    """
    job_role_cols = [col for col in lf.collect_schema().names() if col.startswith("jr")]
    _raise_if_uncatalogued_job_role_codes(job_role_cols)
    existing_job_role_cols = set(job_role_cols)

    job_role_suffixes = sorted({re.sub(r"^jr\d+", "", col) for col in job_role_cols})

    lf = lf.with_columns(
        reduce_to_published_roles_expressions(
            OTHER_ROLE_CODE_TO_UNPUBLISHED_JOB_ROLE_CODES,
            job_role_suffixes,
            existing_job_role_cols,
        ),
    )

    unpublished_roles = [
        code
        for codes in OTHER_ROLE_CODE_TO_UNPUBLISHED_JOB_ROLE_CODES.values()
        for code in codes
    ]
    roles_to_drop = [
        f"jr{role}{suffix}"
        for role in unpublished_roles
        for suffix in job_role_suffixes
    ]
    lf = lf.drop(cs.by_name(*roles_to_drop, require_all=False))

    return lf


def _raise_if_uncatalogued_job_role_codes(job_role_cols: list[str]) -> None:
    """
    Raise if any job role column's code isn't in MainJobRoleID.

    Args:
        job_role_cols (list[str]): Column names starting with 'jr'.

    Raises:
        ValueError: If one or more codes aren't catalogued.
    """
    codes = {re.match(r"^jr(\d+)", col).group(1) for col in job_role_cols}
    unknown_codes = codes - _ALL_CATALOGUED_JOB_ROLE_CODES
    if unknown_codes:
        raise ValueError(
            f"Unrecognised ASC-WDS job role code(s) {sorted(unknown_codes)} found in "
            "input columns. Add them to MainJobRoleID and classify them (either as a "
            "new published role in PublishedJobRoleLabels, or via MainJobRoleLabels + "
            "AscwdsWorkerValueLabelsJobGroup) before running reduce_to_published_roles."
        )


def reduce_to_published_roles_expressions(
    other_role_code_to_unpublished_codes: dict[str, list[str]],
    slv_suffixes: list[str],
    existing_job_role_cols: set[str],
) -> Generator[pl.Expr, None, None]:
    """
    A generator function that yields Polars expressions that sum each other_*
    role's unpublished source columns (across the given suffixes) into that
    other_* role's column.

    When all columns to sum are null then expression produces null. A suffix
    is skipped entirely for a given other_* role if none of its unpublished
    source columns are present for that suffix (e.g. a test fixture, or a
    dataset extract, that doesn't carry every ASC-WDS job role column) -
    `sum_horizontal` requires at least one input.

    Args:
        other_role_code_to_unpublished_codes (dict[str, list[str]]): Mapping of
            synthetic other_* role code (e.g. "1001") to the raw unpublished
            job role codes that fold into it.
        slv_suffixes (list[str]): A list of ASC-WDS workplace job role column suffixes.
            E.g. ["flag", "emp", "work"]
        existing_job_role_cols (set[str]): Job role column names present in the
            input LazyFrame.

    Yields:
        pl.Expr: Polars expressions for summing columns.

    """
    for role_to_keep, roles_to_merge in other_role_code_to_unpublished_codes.items():
        for suffix in slv_suffixes:
            source_cols = [
                f"jr{old}{suffix}"
                for old in roles_to_merge
                if f"jr{old}{suffix}" in existing_job_role_cols
            ]
            if not source_cols:
                continue
            yield (
                pl.when(pl.all_horizontal(pl.col(source_cols).is_null()))
                .then(pl.lit(None))
                .otherwise(pl.sum_horizontal(source_cols))
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
