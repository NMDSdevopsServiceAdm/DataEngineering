import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
    JobGroupLabels,
)


def create_job_role_estimates_data_validation_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Create columns for validating the filled post estimates by job role.
    Columns created are:
        - national_percentage_care_worker_filled_posts
        - national_percentage_direct_care_filled_posts
        - national_percentage_managers_filled_posts
        - national_percentage_regulated_professions_filled_posts
        - national_percentage_other_filled_posts

    Args:
        lf (pl.LazyFrame): The input LazyFrame.

    Returns:
        pl.LazyFrame: The LazyFrame with the new validation columns.
    """
    partition = IndCQC.cqc_location_import_date
    job_role_col = IndCQC.main_job_role_clean_labelled
    value_col = IndCQC.estimate_filled_posts_by_job_role_manager_adjusted

    denominator = (
        pl.col(IndCQC.estimate_filled_posts_from_all_job_roles).max().over(partition)
    )

    job_group_to_roles = {}
    for (
        role,
        group,
    ) in AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.items():
        job_group_to_roles.setdefault(group, []).append(role)

    percentage_columns = [
        (
            IndCQC.national_percentage_care_worker_filled_posts,
            [MainJobRoleLabels.care_worker],
        ),
        (
            IndCQC.national_percentage_direct_care_filled_posts,
            job_group_to_roles[JobGroupLabels.direct_care],
        ),
        (
            IndCQC.national_percentage_managers_filled_posts,
            job_group_to_roles[JobGroupLabels.managers],
        ),
        (
            IndCQC.national_percentage_regulated_professions_filled_posts,
            job_group_to_roles[JobGroupLabels.regulated_professions],
        ),
        (
            IndCQC.national_percentage_other_filled_posts,
            job_group_to_roles[JobGroupLabels.other],
        ),
    ]

    return lf.with_columns(
        (
            pl.when(pl.col(job_role_col).is_in(roles))
            .then(pl.col(value_col))
            .otherwise(0)
            .sum()
            .over(partition)
            / denominator
        )
        .cast(pl.Float32)
        .alias(new_col)
        for new_col, roles in percentage_columns
    )
