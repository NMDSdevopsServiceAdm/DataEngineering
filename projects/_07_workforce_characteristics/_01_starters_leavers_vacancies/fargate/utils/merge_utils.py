import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    PublishedJobRoleLabels,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
    SLVPrepareCategoricalValues,
)

# Roles that exist under the same name in both job-role taxonomies are
# collated into this set object.
ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES = set(
    CatVals.main_job_role_labels_column_values.categorical_values
) & set(
    SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
)


def collapse_job_role_estimates_to_published_labels(
    job_role_estimates_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Collapses job_role_estimates_lf's granular job roles to the published scheme.

    We estimate filled posts for all job roles, but the prepared slv data only
    has rows for published job roles. Therefore, the estimates LazyFrame must be
    aggregated up to the same job role level as prepared slv data before they
    are joined.

    .first() is used to retain columns through the aggregation as each of them
    have the same values per group_by group.

    Args:
        job_role_estimates_lf (pl.LazyFrame): job role estimates for all job
            roles.

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
        .cast(CatColType.PublishedJobRoleLabelCatType)
        .alias(SLVCols.published_job_role_label)
    )

    return published_role_lf.group_by(
        IndCQC.id_per_locationid_import_date, SLVCols.published_job_role_label
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
