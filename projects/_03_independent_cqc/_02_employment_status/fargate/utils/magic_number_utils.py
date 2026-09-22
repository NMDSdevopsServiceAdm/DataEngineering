import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusMagicNumberRateColumns as EmpStatRates,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobRoleLabels,
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
    metric = IndCQC.estimate_filled_posts_by_job_role

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
        .alias(IndCQC.published_job_role_label),
        pl.col(EmpStatRates.emp_stat_perm),
        pl.col(EmpStatRates.emp_stat_temp),
        pl.col(EmpStatRates.emp_stat_bank_or_pool),
        pl.col(EmpStatRates.emp_stat_agency),
        pl.col(EmpStatRates.emp_stat_other),
    )

    job_role_estimates_lf = (
        job_role_estimates_lf.join(
            mapped_rates_lf,
            on=[IndCQC.primary_service_type, IndCQC.published_job_role_label],
            how="left",
        )
        .with_columns(
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_perm)).alias(
                EmpStatus.estimated_emp_stat_perm
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_temp)).alias(
                EmpStatus.estimated_emp_stat_temp
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_bank_or_pool)).alias(
                EmpStatus.estimated_emp_stat_bank_or_pool
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_agency)).alias(
                EmpStatus.estimated_emp_stat_agency
            ),
            (pl.col(metric) * pl.col(EmpStatRates.emp_stat_other)).alias(
                EmpStatus.estimated_emp_stat_other
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
            pl.col(EmpStatus.estimated_emp_stat_perm)
            + pl.col(EmpStatus.estimated_emp_stat_temp)
        ).alias(EmpStatus.estimated_employees)
    )
