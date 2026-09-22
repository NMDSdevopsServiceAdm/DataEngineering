import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    PublishedJobRoleLabels,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
)
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)

# Roles that exist under the same name in both job-role taxonomies are
# collated into this set object.
ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES = set(
    CatVals.main_job_role_labels_column_values.categorical_values
) & set(
    SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
)

JOB_GROUP_TO_OTHER_PUBLISHED_LABEL: dict[str, str] = {
    JobGroupLabels.managers: PublishedJobRoleLabels.other_managers,
    JobGroupLabels.regulated_professions: PublishedJobRoleLabels.other_regulated_professions,
    JobGroupLabels.direct_care: PublishedJobRoleLabels.other_direct_care,
    JobGroupLabels.other: PublishedJobRoleLabels.other,
}

# Same mapping as the when/otherwise below, as a dict - lets worker data (no
# main_job_group_labelled column) resolve a published label via replace_strict too.
JOB_ROLE_LABEL_TO_PUBLISHED_LABEL: dict[str, str] = {
    label: (
        label
        if label in ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES
        else JOB_GROUP_TO_OTHER_PUBLISHED_LABEL[
            AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict[label]
        ]
    )
    for label in CatVals.main_job_role_labels_column_values.categorical_values
}


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
            estimate_filled_posts_by_job_role summed
            across whichever granular roles collapsed into each published label.
    """
    metric = IndCQC.estimate_filled_posts_by_job_role

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
        .alias(IndCQC.published_job_role_label)
    )

    return published_role_lf.group_by(
        IndCQC.id_per_locationid_import_date, IndCQC.published_job_role_label
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
