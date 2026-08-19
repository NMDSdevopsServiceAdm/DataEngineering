import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.employment_status_rates_columns import (
    EmploymentStatusRatesColumns as EmpStatRates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols
from utils.column_values.categorical_column_values import (
    JobGroupLabels,
    PrimaryServiceType,
    PublishedJobRoleLabels,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
)
from utils.column_values.categorical_columns_by_dataset import (
    SLVPrepareCategoricalValues,
)

# Roles that exist under the same name in both job-role taxonomies are
# collated into this set object.
ROLES_SHARED_BY_BOTH_JOB_ROLE_TAXONOMIES = set(
    CatVals.main_job_role_labels_column_values.categorical_values
) & set(
    SLVPrepareCategoricalValues.published_job_role_labels_column_values.categorical_values
)

CSV_SERVICE_TO_PRIMARY_SERVICE_TYPE: dict[str, str] = {
    "CQC Care only home": PrimaryServiceType.care_home_only,
    "CQC Care home with nursing": PrimaryServiceType.care_home_with_nursing,
    "CQC Non residential": PrimaryServiceType.non_residential,
}

CSV_WEIGHTING_JOB_ROLE_TO_PUBLISHED_JOB_ROLE_LABEL: dict[str, str] = {
    "Senior_management": PublishedJobRoleLabels.senior_management,
    "Registered_manager": PublishedJobRoleLabels.registered_manager,
    "Deputy_manager": PublishedJobRoleLabels.deputy_manager,
    "Social_worker": PublishedJobRoleLabels.social_worker,
    "Occupational_therapist": PublishedJobRoleLabels.occupational_therapist,
    "Registered_nurse": PublishedJobRoleLabels.registered_nurse,
    "Allied_health_professional": PublishedJobRoleLabels.allied_health_professional,
    "Senior_care_worker": PublishedJobRoleLabels.senior_care_worker,
    "Care_worker": PublishedJobRoleLabels.care_worker,
    "Support_and_outreach": PublishedJobRoleLabels.community_support_and_outreach,
    "Support_worker": PublishedJobRoleLabels.support_worker,
    "Other_managers": PublishedJobRoleLabels.other_managers,
    "Other_regulated_professions": PublishedJobRoleLabels.other_regulated_professions,
    "Other_direct_care": PublishedJobRoleLabels.other_direct_care,
    "All_others": PublishedJobRoleLabels.other,
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


def apply_employment_status_magic_numbers(
    job_role_estimates_lf: pl.LazyFrame,
    employment_status_rates_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Splits the job-role filled-post estimate into estimated employment-status
    components.

    Temporary stopgap ahead of a dedicated employment status estimation
    pipeline, expected to be removed within a few months. Multiplies the
    filled-post metric by each of the CSV's employment-status rates (assumed to
    sum to ~1 per row) to produce 5 estimated-filled-post-by-status columns,
    then adds an estimated_employees column.

    Args:
        job_role_estimates_lf (pl.LazyFrame): merged job role estimates, already
            joined to the cleaned ASCWDS workplace data, with primary_service_type,
            job_role_label, the filled-post metric and the ASCWDS employees column.
        employment_status_rates_lf (pl.LazyFrame): the employment status rates CSV
            data, keyed by service and weighting_job_role.

    Returns:
        pl.LazyFrame: job_role_estimates_lf with 5 new
            estimated-filled-post-by-employment-status
            columns and an estimated_employees column.
    """
    metric = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated

    mapped_rates_lf = employment_status_rates_lf.select(
        pl.col(EmpStatRates.service)
        .cast(pl.String)
        .replace_strict(CSV_SERVICE_TO_PRIMARY_SERVICE_TYPE)
        .cast(CatColType.PrimaryServiceEnumType)
        .alias(IndCQC.primary_service_type),
        pl.col(EmpStatRates.weighting_job_role)
        .cast(pl.String)
        .replace_strict(CSV_WEIGHTING_JOB_ROLE_TO_PUBLISHED_JOB_ROLE_LABEL)
        .cast(CatColType.PublishedJobRoleLabelCatType)
        .alias(SLVCols.published_job_role_label),
        pl.col(EmpStatRates.emp_stat_perm),
        pl.col(EmpStatRates.emp_stat_temp),
        pl.col(EmpStatRates.emp_stat_bank_or_pool),
        pl.col(EmpStatRates.emp_stat_agency),
        pl.col(EmpStatRates.emp_stat_other),
    )

    job_role_estimates_lf = (
        job_role_estimates_lf.join(
            mapped_rates_lf,
            on=[IndCQC.primary_service_type, SLVCols.published_job_role_label],
            how="left",
        )
        .with_columns(
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_perm)).alias(
                SLVCols.estimated_emp_stat_perm
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_temp)).alias(
                SLVCols.estimated_emp_stat_temp
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_bank_or_pool)).alias(
                SLVCols.estimated_emp_stat_bank_or_pool
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_agency)).alias(
                SLVCols.estimated_emp_stat_agency
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_other)).alias(
                SLVCols.estimated_emp_stat_other
            ),
        )
        .drop(
            EmpStatRates.emp_stat_perm,
            EmpStatRates.emp_stat_temp,
            EmpStatRates.emp_stat_bank_or_pool,
            EmpStatRates.emp_stat_agency,
            EmpStatRates.emp_stat_other,
        )
    )

    return job_role_estimates_lf.with_columns(
        (
            pl.col(SLVCols.estimated_emp_stat_perm)
            + pl.col(SLVCols.estimated_emp_stat_temp)
        ).alias(SLVCols.estimated_employees)
    )
