import re

import polars as pl

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


def reduce_to_published_roles():
    """
    Placeholder function to reduce columns to only published roles plus
    other_dc/other_man etc.
    """
    pass


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
    produced by merge_job_role_columns are looked up in
    SYNTHETIC_JOB_ROLE_LABELS instead. Columns not matching the jrNN{suffix}
    shape are left untouched.

    Args:
        lf (pl.LazyFrame): LazyFrame with jrNN{suffix} columns, after the
            job-role merge and before any pivot.

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
