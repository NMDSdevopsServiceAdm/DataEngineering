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
# (EstimatedIndCQCFilledPostsByJobRoleCategoricalValues) and the 15-category published
# scheme cleaned_ascwds_workplace_lf.job_role_label uses (SLVPrepareCategoricalValues).
# Every other granular role has no equivalent in the published scheme and must be
# bucketed into one of the 4 'other_*' groups instead.
ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES = {
    PublishedJobRoleLabels.allied_health_professional,
    PublishedJobRoleLabels.care_worker,
    PublishedJobRoleLabels.community_support_and_outreach,
    PublishedJobRoleLabels.deputy_manager,
    PublishedJobRoleLabels.occupational_therapist,
    PublishedJobRoleLabels.registered_manager,
    PublishedJobRoleLabels.registered_nurse,
    PublishedJobRoleLabels.senior_care_worker,
    PublishedJobRoleLabels.senior_management,
    PublishedJobRoleLabels.social_worker,
    PublishedJobRoleLabels.support_worker,
}


def collapse_job_role_estimates_to_published_labels(
    job_role_estimates_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Collapses job_role_estimates_lf's granular job roles to the published scheme.

    main_job_role_clean_labelled and cleaned_ascwds_workplace_lf's job_role_label
    are independently-evolved Enum taxonomies (37 granular roles vs 15 published
    roles) with only 11 role names in common, so they can't be joined directly.
    Roles outside that overlap are bucketed into one of the 4 published 'other_*'
    groups via the row's existing main_job_group_labelled column, then summed
    together - mirroring prepare_utils.reduce_to_published_roles' "null only if
    every contributing row is null" convention for the same other-bucket concept
    on the workplace side.

    Must run before job_role_estimates_lf is joined to metadata_lf: location_id,
    cqc_location_import_date, primary_service_type, and main_job_group_labelled
    are all functionally dependent on id_per_locationid_import_date, so grouping
    here (rather than after metadata_lf adds many more such columns) avoids a
    `.first()` aggregation per metadata column.

    Args:
        job_role_estimates_lf (pl.LazyFrame): job role estimates, one row per
            location/import-date/granular-job-role.

    Returns:
        pl.LazyFrame: one row per location/import-date/published-job-role, with
            estimate_filled_posts_by_job_role_historically_reallocated summed
            across whichever granular roles collapsed into each published label.
    """
    metric = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated

    published_role_lf = job_role_estimates_lf.with_columns(
        pl.when(
            pl.col(IndCQC.main_job_role_clean_labelled)
            .cast(pl.String)
            .is_in(ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES)
        )
        .then(pl.col(IndCQC.main_job_role_clean_labelled).cast(pl.String))
        # main_job_group_labelled -> other_* bucket, written as an explicit when/then
        # chain rather than .replace(): .replace() falls back to the in-memory engine
        # under engine="streaming" (see the polars-streaming-check skill), which this
        # scale-sensitive pipeline can't afford. The final .otherwise(other) catch-all
        # is safe (not silently swallowing unmapped values) because main_job_group_labelled
        # is already an exhaustively-populated 4-value domain by this point - it's
        # produced upstream via AscwdsWorkerValueLabelsJobGroup, which raises for any
        # unmapped role at that stage.
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


def join_datasets(
    job_role_estimates_lf: pl.LazyFrame, cleaned_ascwds_workplace_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Left-joins SLV starters/leavers/vacancies/employees onto job role estimates.

    job_role_estimates_lf must already be collapsed to the published job role
    scheme (see collapse_job_role_estimates_to_published_labels) and carry
    establishment_id/ascwds_workplace_import_date (added by the metadata_lf join
    upstream), since those plus job_role_label form the join key.
    establishment_id/location_id is 1:1 in this data, so the join doesn't fan out.

    Args:
        job_role_estimates_lf (pl.LazyFrame): job role estimates, collapsed to
            published job roles and joined to metadata.
        cleaned_ascwds_workplace_lf (pl.LazyFrame): SLV workplace data, one row
            per establishment/import-date/published-job-role.

    Returns:
        pl.LazyFrame: job_role_estimates_lf with employees, starters, leavers,
            and vacancies columns added; null where no matching workplace row
            exists.
    """
    return job_role_estimates_lf.join(
        cleaned_ascwds_workplace_lf,
        on=[
            IndCQC.establishment_id,
            IndCQC.ascwds_workplace_import_date,
            SLVCols.job_role_label,
        ],
        how="left",
    )


def apply_employment_status_magic_numbers():
    """
    Placeholder function to apply employment status magic numbers."""
    pass
