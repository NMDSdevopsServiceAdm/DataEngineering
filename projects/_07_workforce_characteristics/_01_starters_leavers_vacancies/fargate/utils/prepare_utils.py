import re

import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
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

SYNTHETIC_JOB_ROLE_LABELS = {
    "1001": PublishedJobRoleLabels.other_managers,
    "1002": PublishedJobRoleLabels.other_regulated_professions,
    "1003": PublishedJobRoleLabels.other_direct_care,
    "1004": PublishedJobRoleLabels.other,
}


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


def reshape_job_role_cols_to_rows(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Reshapes wide per-job-role columns into one row per job role.

    Must run after relabel_job_role_columns, which guarantees every job role
    column's prefix is one of the known PublishedJobRoleLabels values - this
    function trusts that instead of pattern-matching raw codes. Of the grain
    columns (establishment_id, ascwds_workplace_import_date, job_role_label),
    establishment_id and job_role_label are Categorical/Enum rather than
    String to keep downstream uniqueness checks cheap at this dataset's
    scale.

    Args:
        lf (pl.LazyFrame): LazyFrame with columns already relabelled to
            {published_label}_{suffix} shape.

    Returns:
        pl.LazyFrame: long-format LazyFrame with columns establishment_id,
            ascwds_workplace_import_date, job_role_label, employees,
            starters, leavers, vacancies. One row per label per input row
            (dense - includes all-null metric rows).
    """
    metric_suffixes = {
        SLVCols.employees: "emp",
        SLVCols.starters: "strt",
        SLVCols.leavers: "stop",
        SLVCols.vacancies: "vacy",
    }

    # One struct per published label, e.g. {job_role_label: "care_worker",
    # employees: 5, starters: 1, leavers: 0, vacancies: 2} - concat_list+explode
    # below turns this list-of-structs-per-row into one row per label.
    label_structs = [
        pl.struct(
            pl.lit(label).alias(SLVCols.job_role_label),
            *[
                pl.col(f"{label}_{suffix}").alias(metric)
                for metric, suffix in metric_suffixes.items()
            ],
        )
        for label in published_job_role_labels
    ]

    return (
        lf.select(
            AWPClean.establishment_id,
            AWPClean.ascwds_workplace_import_date,
            pl.concat_list(label_structs).alias("_job_role_struct_list"),
        )
        .explode("_job_role_struct_list")
        .unnest("_job_role_struct_list")
        .with_columns(
            pl.col(AWPClean.establishment_id).cast(CatColType.EstablishmentCatType),
            pl.col(SLVCols.job_role_label).cast(
                CatColType.PublishedJobRoleLabelEnumType
            ),
            pl.col(SLVCols.employees).cast(pl.Int16),
            pl.col(SLVCols.starters).cast(pl.Int16),
            pl.col(SLVCols.leavers).cast(pl.Int16),
            pl.col(SLVCols.vacancies).cast(pl.Int16),
        )
    )


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
            published roles, before any reshape.

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
