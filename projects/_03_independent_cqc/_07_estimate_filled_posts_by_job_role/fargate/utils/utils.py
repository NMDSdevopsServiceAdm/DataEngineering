import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as AscwdsJobRoles,
)

LIST_OF_JOB_ROLES_SORTED = sorted(list(AscwdsJobRoles.labels_dict.values()))


def aggregate_ascwds_worker_job_roles_per_establishment(
    lf: pl.LazyFrame, list_of_job_roles: list
) -> list[pl.DataFrame]:
    """
    Counts rows in the worker dataset by establishment_id, ascwds_worker_import_date and main_job_role_clean_labelled.

    This function aggregates the worker dataset by establishment_id, ascwds_worker_import_date and main_job_role_clean_labelled
    to get a job role count per workplace. Then explodes a list of all potential job roles per row, and finally deduplicates
    rows on the exploded job role column by aggregating to get the sum of job role count per role.
    So if an establishment does not have particular role, then their count for that role is 0.
    All establishments end up with a row for all potential job roles.

    Args:
        lf (pl.LazyFrame): A dataframe containing cleaned ASC-WDS worker data.
        list_of_job_roles (list): A list of job roles in alphabetical order.

    Returns:
        list[pl.DataFrame]: The input dataframe with a count of rows for all potential job roles.
    """
    unique_workplaces_lf = lf.unique(
        [
            pl.col(IndCQC.establishment_id),
            pl.col(IndCQC.ascwds_worker_import_date),
        ]
    ).drop(IndCQC.main_job_role_clean_labelled)

    all_job_roles_lf = pl.LazyFrame(
        {
            IndCQC.main_job_role_clean_labelled: list_of_job_roles,
            IndCQC.ascwds_job_role_counts: [0] * len(list_of_job_roles),
        }
    )

    unique_workplaces_lf = unique_workplaces_lf.join(
        other=all_job_roles_lf, how="cross"
    )

    unique_workplaces_lf.rename(
        {IndCQC.ascwds_job_role_counts + "_right": IndCQC.ascwds_job_role_counts}
    )

    worker_count_lf = lf.group_by(
        [
            pl.col(IndCQC.establishment_id),
            pl.col(IndCQC.ascwds_worker_import_date),
            pl.col(IndCQC.main_job_role_clean_labelled),
        ]
    ).len(name=IndCQC.ascwds_job_role_counts)

    unique_workplaces_lf = unique_workplaces_lf.join(
        other=worker_count_lf,
        on=[
            pl.col(IndCQC.establishment_id),
            pl.col(IndCQC.ascwds_worker_import_date),
            pl.col(IndCQC.main_job_role_clean_labelled),
        ],
        how="left",
    )

    unique_workplaces_lf = unique_workplaces_lf.fill_null(0)

    unique_workplaces_lf = unique_workplaces_lf.drop(
        IndCQC.ascwds_job_role_counts
    ).rename({IndCQC.ascwds_job_role_counts + "_right": IndCQC.ascwds_job_role_counts})

    unique_workplaces_lf = unique_workplaces_lf.select(
        [
            IndCQC.establishment_id,
            IndCQC.ascwds_worker_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.ascwds_job_role_counts,
        ]
    )

    return pl.collect_all([unique_workplaces_lf, worker_count_lf])


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
        estimated_filled_posts_lf (pl.LazyFrame): A dataframe containing estimated filled posts at workplace level.
        aggregated_job_roles_per_establishment_lf (pl.LazyFrame): ASC-WDS job role breakdown dataframe aggregated at workplace level.

    Returns:
        pl.LazyFrame: The estimated filled post DataFrame with the job role count map column joined in.
    """

    merged_lf = estimated_filled_posts_lf.join(
        other=aggregated_job_roles_per_establishment_lf,
        left_on=[IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date],
        right_on=[IndCQC.establishment_id, IndCQC.ascwds_worker_import_date],
        how="left",
    )

    return merged_lf
