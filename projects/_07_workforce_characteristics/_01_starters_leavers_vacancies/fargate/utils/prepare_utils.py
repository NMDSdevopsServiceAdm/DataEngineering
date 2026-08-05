import re

import polars as pl

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
ALL_CATALOGUED_JOB_ROLE_CODES = {code.zfill(2) for code in _role_id_by_name.values()}

# Published roles are left untouched by reduce_to_published_roles.
PUBLISHED_JOB_ROLE_CODES = {
    _role_id_by_name[name].zfill(2) for name in _published_names
}

_other_role_code_by_job_group = {
    JobGroupLabels.managers: "1001",
    JobGroupLabels.regulated_professions: "1002",
    JobGroupLabels.direct_care: "1003",
    JobGroupLabels.other: "1004",
}

# Raw code -> synthetic "other_*" job role code it folds into (jr1001-jr1004;
# not real ASC-WDS codes). Codes absent from this dict - published roles, and
# `technician`(22)/`care_navigator`(41) which have no MainJobRoleLabels/job-group
# entry - are left untouched by reduce_to_published_roles. technician/
# care_navigator are already merged upstream into other codes by
# clean_ascwds_workplace.py's legacy_job_roles_dict during ASCWDS ingest
# cleaning, so they never reach this function as their own columns in practice.
#
# NOTE: ticket 1794 (separate, unmerged branch) independently introduces its
# own reference to these same 4 codes for a later column-relabelling step -
# this is deliberate, accepted duplication, not to be unified here.
CODE_TO_OTHER_ROLE_CODE = {
    _role_id_by_name[name].zfill(2): _other_role_code_by_job_group[
        AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict[label]
    ]
    for name, label in _role_label_by_name.items()
    if name not in _published_names
}

_JOB_ROLE_COLUMN_PATTERN = re.compile(r"^jr(\d+)([a-z]+)$")


def reduce_to_published_roles(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merge ASC-WDS workplace job role columns down to published roles.

    Published job roles (PublishedJobRoleLabels) are left untouched. Every
    other catalogued raw job role (MainJobRoleID) is summed into whichever of
    the 4 'other_*' synthetic roles (jr1001/jr1002/jr1003/jr1004) its job group
    (AscwdsWorkerValueLabelsJobGroup) maps to, then dropped. A sum is null only
    when all of its contributing columns are null.

    Args:
        lf (pl.LazyFrame): ASC-WDS workplace LazyFrame.

    Returns:
        pl.LazyFrame: Input LazyFrame in which unpublished job role columns
            have been merged into the other_* roles and removed.

    Raises:
        ValueError: If a job role column's code isn't in MainJobRoleID (an
            ASC-WDS code this team hasn't catalogued yet).
    """
    job_role_matches = {
        col: _JOB_ROLE_COLUMN_PATTERN.match(col)
        for col in lf.collect_schema().names()
        if col.startswith("jr")
    }

    unknown_codes = {
        match.group(1) for match in job_role_matches.values()
    } - ALL_CATALOGUED_JOB_ROLE_CODES
    if unknown_codes:
        raise ValueError(
            f"Unrecognised ASC-WDS job role code(s) {sorted(unknown_codes)} found in "
            "input columns. Add them to MainJobRoleID and classify them (either as a "
            "new published role in PublishedJobRoleLabels, or via MainJobRoleLabels + "
            "AscwdsWorkerValueLabelsJobGroup) before running reduce_to_published_roles."
        )

    merge_groups: dict[tuple[str, str], list[str]] = {}
    for col, match in job_role_matches.items():
        code, suffix = match.groups()
        other_role_code = CODE_TO_OTHER_ROLE_CODE.get(code)
        if other_role_code:
            merge_groups.setdefault((other_role_code, suffix), []).append(col)

    lf = lf.with_columns(
        pl.when(pl.all_horizontal(pl.col(source_cols).is_null()))
        .then(pl.lit(None))
        .otherwise(pl.sum_horizontal(source_cols))
        .alias(f"jr{other_role_code}{suffix}")
        for (other_role_code, suffix), source_cols in merge_groups.items()
    )
    lf = lf.drop([col for source_cols in merge_groups.values() for col in source_cols])

    return lf


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
