import re

import polars as pl

from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    PublishedJobRoleLabels,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)

_other_role_code_by_job_group = {
    JobGroupLabels.managers: "1001",
    JobGroupLabels.regulated_professions: "1002",
    JobGroupLabels.direct_care: "1003",
    JobGroupLabels.other: "1004",
}

# PublishedJobRoleLabels' 4 synthetic other_* fields aren't real ASCWDS labels,
# so intersecting with real job-role labels drops them without needing to name
# them.
_published_labels = {
    value
    for name, value in vars(PublishedJobRoleLabels).items()
    if not name.startswith("_")
} & set(AscwdsWorkerValueLabelsMainjrid.labels_dict.values())

# Every ASC-WDS job role code with a known label and job-group classification,
# zero-padded to match real column names (labels_dict stores bare codes, e.g.
# "1", not "01"). `technician`(22)/`care_navigator`(41) are real MainJobRoleID
# codes with no label/job-group entry here - they're already merged upstream
# into other codes by clean_ascwds_workplace.py's legacy_job_roles_dict during
# ASCWDS ingest cleaning, so they're not expected to reach this function; if
# they ever did, they should error the same as any other uncatalogued code.
ALL_CATALOGUED_JOB_ROLE_CODES = {
    code.zfill(2) for code in AscwdsWorkerValueLabelsMainjrid.labels_dict
}

# Published roles are left untouched by reduce_to_published_roles.
PUBLISHED_JOB_ROLE_CODES = {
    code.zfill(2)
    for code, label in AscwdsWorkerValueLabelsMainjrid.labels_dict.items()
    if label in _published_labels
}

# Raw code -> synthetic "other_*" job role code it folds into (jr1001-jr1004;
# not real ASC-WDS codes). Codes absent from this dict - published roles, and
# the technician/care_navigator gap above - are left untouched by
# reduce_to_published_roles.
#
# NOTE: ticket 1794 (separate, unmerged branch) independently introduces its
# own reference to these same 4 codes for a later column-relabelling step -
# this is deliberate, accepted duplication, not to be unified here.
CODE_TO_OTHER_ROLE_CODE = {
    code.zfill(2): _other_role_code_by_job_group[
        AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict[label]
    ]
    for code, label in AscwdsWorkerValueLabelsMainjrid.labels_dict.items()
    if label not in _published_labels
}

_JOB_ROLE_COLUMN_PATTERN = re.compile(r"^jr(\d+)([a-z]+)$")


def reduce_to_published_roles(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Merge ASC-WDS workplace job role columns down to published roles.

    Published job roles (PublishedJobRoleLabels) are left untouched. Every
    other catalogued raw job role (AscwdsWorkerValueLabelsMainjrid) is summed
    into whichever of the 4 'other_*' synthetic roles (jr1001/jr1002/jr1003/
    jr1004) its job group (AscwdsWorkerValueLabelsJobGroup) maps to, then
    dropped. A sum is null only when all of its contributing columns are null.

    Args:
        lf (pl.LazyFrame): ASC-WDS workplace LazyFrame.

    Returns:
        pl.LazyFrame: Input LazyFrame in which unpublished job role columns
            have been merged into the other_* roles and removed.

    Raises:
        ValueError: If a job role column's code isn't in
            AscwdsWorkerValueLabelsMainjrid.labels_dict (an ASC-WDS code this
            team hasn't catalogued yet).
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
            "input columns. Add them to AscwdsWorkerValueLabelsMainjrid.labels_dict and "
            "classify them (either as a new published role in PublishedJobRoleLabels, "
            "or via AscwdsWorkerValueLabelsJobGroup) before running "
            "reduce_to_published_roles."
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
