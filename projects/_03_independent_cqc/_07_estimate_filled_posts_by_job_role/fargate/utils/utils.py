import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as AscwdsJobRoles,
)

LIST_OF_JOB_ROLES_SORTED = sorted(list(AscwdsJobRoles.labels_dict.values()))


def aggregate_ascwds_worker_job_roles_per_establishment(
    lf: pl.LazyFrame, list_of_job_roles: list
) -> pl.LazyFrame:
    """
    Counts rows in the worker dataset by establishment_id, ascwds_worker_import_date and main_job_role_clean_labelled.

    This function aggregates the worker dataset by establishment_id, ascwds_worker_import_date and main_job_role_clean_labelled
    to get a job role count per workplace. A row is added for all potential job roles per workplace, and if a workplace
    does not have any of a particular role then the count is 0.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing cleaned ASC-WDS worker data.
        list_of_job_roles (list): A list of job roles in alphabetical order.

    Returns:
        pl.LazyFrame: The input LazyFrame with a new column for the count job roles per workplace.
    """
    columns = [
        IndCQC.establishment_id,
        IndCQC.ascwds_worker_import_date,
        IndCQC.main_job_role_clean_labelled,
    ]

    # Aggregate worker data into one row per job role per workplace with a count column.
    worker_count_lf = lf.group_by(columns).len(name=IndCQC.ascwds_job_role_counts)

    # Pivot the job role labels into columns, with the counts as their values, and add columns for all potential job roles.
    aggregation = [
        (pl.col(IndCQC.main_job_role_clean_labelled) == role).sum().alias(role)
        for role in list_of_job_roles
    ]

    worker_count_lf = worker_count_lf.group_by(
        [col for col in columns if col not in [IndCQC.main_job_role_clean_labelled]]
    ).agg(aggregation)

    # Pivot all the job role columns into rows.
    worker_count_lf = worker_count_lf.unpivot(
        index=[
            IndCQC.establishment_id,
            IndCQC.ascwds_worker_import_date,
        ],
        on=[role for role in list_of_job_roles],
        variable_name=IndCQC.main_job_role_clean_labelled,
        value_name=IndCQC.ascwds_job_role_counts,
    )

    return worker_count_lf


def join_worker_to_estimates_dataframe(
    estimated_filled_posts_lf: pl.LazyFrame,
    aggregated_job_roles_per_establishment_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Join the mainjrid_clean_labels and ascwds_job_role_counts columns from the aggregated worker LazyFrame into the estimated filled post LazyFrame.

    Join as left join where:
      left = estimated filled post LazyFrame
      right = aggregated worker LazyFrame
      where establishment_id matches and ascwds_workplace_import_date == ascwds_worker_import_date.

    Args:
        estimated_filled_posts_lf (pl.LazyFrame): A LazyFrame containing estimated filled posts per workplace.
        aggregated_job_roles_per_establishment_lf (pl.LazyFrame): A LazyFrame with job role counts per workplace.

    Returns:
        pl.LazyFrame: The estimated filled post LazyFrame with the job role count column.
    """

    merged_lf = estimated_filled_posts_lf.join(
        other=aggregated_job_roles_per_establishment_lf,
        left_on=[IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date],
        right_on=[IndCQC.establishment_id, IndCQC.ascwds_worker_import_date],
        how="left",
    )

    return merged_lf
