import re

import polars as pl

from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    PublishedJobRoleLabels,
)
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid,
)


other_role_code_by_job_group = {
    JobGroupLabels.managers: "1001",
    JobGroupLabels.regulated_professions: "1002",
    JobGroupLabels.direct_care: "1003",
    JobGroupLabels.other: "1004",
}

# Excludes codes clean_ascwds_workplace.py's legacy_job_roles_dict already merges
# upstream during ASCWDS ingest cleaning (e.g. `technician`(22)/`care_navigator`(41)),
# since they're not expected to reach this stage as their own columns.
all_zero_filled_job_role_codes_and_labels: dict[str, str] = {
    code.zfill(2): label
    for code, label in AscwdsWorkerValueLabelsMainjrid.labels_dict.items()
}
published_job_role_labels = (
    SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
)
published_job_role_codes_and_labels: dict[str, str] = {
    code: label
    for code, label in all_zero_filled_job_role_codes_and_labels.items()
    if label in published_job_role_labels
}

job_role_code_to_other_bucket_code: dict[str, str] = {}
for _code, _label in all_zero_filled_job_role_codes_and_labels.items():
    if _label in published_job_role_labels:
        continue
    if _label not in AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict:
        raise KeyError(
            f"Job role label {_label!r} (code {_code}) is in "
            "AscwdsWorkerValueLabelsMainjrid.labels_dict but has no entry in "
            "AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict. Add it there, "
            "or as a new published role in PublishedJobRoleLabels, before this module "
            "can be imported."
        )
    _job_group = AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict[_label]
    job_role_code_to_other_bucket_code[_code] = other_role_code_by_job_group[_job_group]

JOB_ROLE_COLUMN_PATTERN = re.compile(r"^jr(\d+)([a-z]+)$")


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
        col: JOB_ROLE_COLUMN_PATTERN.match(col)
        for col in lf.collect_schema().names()
        if col.startswith("jr")
    }

    unknown_codes = {
        match.group(1) for match in job_role_matches.values()
    } - all_zero_filled_job_role_codes_and_labels.keys()
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
        other_role_code = job_role_code_to_other_bucket_code.get(code)
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
