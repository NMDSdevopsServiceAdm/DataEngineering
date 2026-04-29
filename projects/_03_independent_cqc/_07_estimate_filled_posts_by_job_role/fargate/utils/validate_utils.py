import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
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
    denominator = (
        pl.col(IndCQC.estimate_filled_posts_from_all_job_roles).sum().over(partition)
    )

    percentage_columns = [
        (
            IndCQC.national_percentage_care_worker_filled_posts,
            MainJobRoleLabels.care_worker,
        ),
        (
            IndCQC.national_percentage_direct_care_filled_posts,
            JobGroupLabels.direct_care,
        ),
        (IndCQC.national_percentage_managers_filled_posts, JobGroupLabels.managers),
        (
            IndCQC.national_percentage_regulated_professions_filled_posts,
            JobGroupLabels.regulated_professions,
        ),
        (IndCQC.national_percentage_other_filled_posts, JobGroupLabels.other),
    ]

    return lf.with_columns(
        (pl.col(col).sum().over(partition) / denominator)
        .cast(pl.Float32)
        .alias(new_col)
        for new_col, col in percentage_columns
    )
