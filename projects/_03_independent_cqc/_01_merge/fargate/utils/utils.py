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
    original_columns = [
        IndCQC.establishment_id,
        IndCQC.ascwds_worker_import_date,
        IndCQC.main_job_role_clean_labelled,
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    worker_count_lf = lf.group_by(original_columns).len(
        name=IndCQC.ascwds_job_role_counts
    )

    pivot_columns = [
        col
        for col in original_columns
        if col not in [IndCQC.main_job_role_clean_labelled]
    ]
    aggregation = [
        pl.col(IndCQC.ascwds_job_role_counts)
        .filter(pl.col(IndCQC.main_job_role_clean_labelled) == role)
        .sum()
        .alias(role)
        for role in list_of_job_roles
    ]
    worker_count_lf = worker_count_lf.group_by(pivot_columns).agg(aggregation)

    unpivot_columns = [
        col
        for col in original_columns
        if col
        not in [IndCQC.main_job_role_clean_labelled, IndCQC.ascwds_job_role_counts]
    ]
    worker_count_lf = worker_count_lf.unpivot(
        index=unpivot_columns,
        on=[role for role in list_of_job_roles],
        variable_name=IndCQC.main_job_role_clean_labelled,
        value_name=IndCQC.ascwds_job_role_counts,
    )

    return worker_count_lf
