import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
    JobGroupLabels,
)
from utils.column_names.validation_table_columns import Validation as validationColumns


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
    new_col = "new_col"
    roles = "roles"

    job_group_to_roles = {}
    for (
        role,
        group,
    ) in AscwdsWorkerValueLabelsJobGroup.job_role_to_job_group_dict.items():
        job_group_to_roles.setdefault(group, []).append(role)

    percentage_columns = [
        {
            new_col: IndCQC.national_percentage_care_worker_filled_posts,
            roles: [MainJobRoleLabels.care_worker],
        },
        {
            new_col: IndCQC.national_percentage_direct_care_filled_posts,
            roles: job_group_to_roles[JobGroupLabels.direct_care],
        },
        {
            new_col: IndCQC.national_percentage_managers_filled_posts,
            roles: job_group_to_roles[JobGroupLabels.managers],
        },
        {
            new_col: IndCQC.national_percentage_regulated_professions_filled_posts,
            roles: job_group_to_roles[JobGroupLabels.regulated_professions],
        },
        {
            new_col: IndCQC.national_percentage_other_filled_posts,
            roles: job_group_to_roles[JobGroupLabels.other],
        },
    ]

    total_records = lf.select(pl.len()).collect().item()
    return lf.group_by(partition).agg(
        *(
            (
                pl.when(pl.col(job_role_col).is_in(col[roles]))
                .then(pl.col(value_col))
                .otherwise(0)
                .sum()
                / pl.sum(value_col)
            )
            .cast(pl.Float32)
            .alias(col[new_col])
            for col in percentage_columns
        ),
        pl.lit(total_records).alias(validationColumns.total_job_role_records),
    )
