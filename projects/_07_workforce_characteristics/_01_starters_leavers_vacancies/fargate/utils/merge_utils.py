import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    PublishedJobRoleLabels,
)

# Roles that exist under the same name in both job-role taxonomies in play here: the
# 37-category scheme job_role_estimates_lf.main_job_role_clean_labelled uses
# (CatColType.JobRoleEnumType) and the 15-category published scheme
# cleaned_ascwds_workplace_lf.job_role_label uses (CatColType.PublishedJobRoleLabelEnumType).
# Derived from the two Enums' own category lists (rather than hand-maintained) so this
# stays correct if either taxonomy changes - mirrors how
# prepare_utils.reduce_to_published_roles derives its own published/unpublished split.
# Every role outside this overlap has no equivalent in the published scheme and must be
# bucketed into one of the 4 'other_*' groups instead.
ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES = set(
    CatColType.JobRoleEnumType.categories
) & set(CatColType.PublishedJobRoleLabelEnumType.categories)


def collapse_job_role_estimates_to_published_labels(
    job_role_estimates_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Collapses job_role_estimates_lf's granular job roles to the published scheme.

    main_job_role_clean_labelled and cleaned_ascwds_workplace_lf's job_role_label
    are independently-evolved Enum taxonomies (37 granular roles vs 15 published
    roles) with only 11 role names in common, so they can't be joined directly.
    Roles outside that overlap are bucketed into one of the 4 published 'other_*'
    groups via the row's existing main_job_group_labelled column, then summed
    together - mirroring prepare_utils.reduce_to_published_roles' "null only if
    every contributing row is null" convention for the same other-bucket concept
    on the workplace side.

    Must run before job_role_estimates_lf is joined to metadata_lf: location_id,
    cqc_location_import_date and primary_service_type are all functionally
    dependent on id_per_locationid_import_date, so grouping here (rather than
    after metadata_lf adds many more such columns) avoids a `.first()`
    aggregation per metadata column.

    Args:
        job_role_estimates_lf (pl.LazyFrame): job role estimates, one row per
            location/import-date/granular-job-role.

    Returns:
        pl.LazyFrame: one row per location/import-date/published-job-role, with
            estimate_filled_posts_by_job_role_historically_reallocated summed
            across whichever granular roles collapsed into each published label.
    """
    metric = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated

    # polars_streaming: .replace() falls back to the in-memory engine therefore
    # when/then chain has been used instead.

    # .otherwise(other) is safe here because job role estimates validation checks
    #  main_job_group_labelled has expected job group labels.

    published_role_lf = job_role_estimates_lf.with_columns(
        pl.when(
            pl.col(IndCQC.main_job_role_clean_labelled)
            .cast(pl.String)
            .is_in(ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES)
        )
        .then(pl.col(IndCQC.main_job_role_clean_labelled).cast(pl.String))
        .otherwise(
            pl.when(pl.col(IndCQC.main_job_group_labelled) == JobGroupLabels.managers)
            .then(pl.lit(PublishedJobRoleLabels.other_managers))
            .when(
                pl.col(IndCQC.main_job_group_labelled)
                == JobGroupLabels.regulated_professions
            )
            .then(pl.lit(PublishedJobRoleLabels.other_regulated_professions))
            .when(pl.col(IndCQC.main_job_group_labelled) == JobGroupLabels.direct_care)
            .then(pl.lit(PublishedJobRoleLabels.other_direct_care))
            .otherwise(pl.lit(PublishedJobRoleLabels.other))
        )
        .cast(CatColType.PublishedJobRoleLabelEnumType)
        .alias(SLVCols.job_role_label)
    )

    return published_role_lf.group_by(
        IndCQC.id_per_locationid_import_date, SLVCols.job_role_label
    ).agg(
        pl.col(IndCQC.location_id).first(),
        pl.col(IndCQC.cqc_location_import_date).first(),
        pl.col(IndCQC.primary_service_type).first(),
        pl.col(IndCQC.main_job_group_labelled).first(),
        pl.when(pl.col(metric).is_null().all())
        .then(pl.lit(None))
        .otherwise(pl.col(metric).sum())
        .alias(metric),
    )


def apply_employment_status_magic_numbers():
    """
    Placeholder function to apply employment status magic numbers."""
    pass
