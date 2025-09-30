import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.value_labels.ascwds_worker.ascwds_worker_mainjrid import (
    AscwdsWorkerValueLabelsMainjrid as AscwdsJobRoles,
)

LIST_OF_JOB_ROLES_SORTED = sorted(list(AscwdsJobRoles.labels_dict.values()))


def aggregate_ascwds_worker_job_roles_per_establishment(
    df: pl.LazyFrame, list_of_job_roles: list
) -> pl.LazyFrame:
    """
    Counts rows in the worker dataset by establishment_id, ascwds_worker_import_date and main_job_role_clean_labelled.

    This function aggregates the worker dataset by establishment_id, ascwds_worker_import_date and main_job_role_clean_labelled
    then cross joins to a dataframe with all job roles and counts set to 0.
    So if an establishment does not have particular role, then their count is 0.
    All establishments end up with a row for all potential job roles.

    Args:
        df (pl.LazyFrame): A dataframe containing cleaned ASC-WDS worker data.
        list_of_job_roles (list): A list of job roles in alphabetical order.

    Returns:
        pl.LazyFrame: The input dataframe with a count of rows for all potential job roles.
    """
    df = df.group_by(
        [
            pl.col(IndCQC.establishment_id),
            pl.col(IndCQC.ascwds_worker_import_date),
            pl.col(IndCQC.main_job_role_clean_labelled),
        ]
    ).len(name=IndCQC.ascwds_job_role_counts)

    all_job_roles_df = pl.LazyFrame(
        {
            IndCQC.main_job_role_clean_labelled: list_of_job_roles,
            IndCQC.ascwds_job_role_counts: [0] * len(list_of_job_roles),
        }
    )

    df = df.join(all_job_roles_df, how="cross")

    df = df.with_columns(
        (
            pl.when(
                pl.col(IndCQC.main_job_role_clean_labelled)
                == pl.col("mainjrid_clean_labels_right")
            )
            .then(pl.col(IndCQC.ascwds_job_role_counts))
            .otherwise(pl.col("ascwds_job_role_counts_right"))
        ).alias(IndCQC.ascwds_job_role_counts)
    )

    df = df.with_columns(
        pl.col("mainjrid_clean_labels_right").alias(IndCQC.main_job_role_clean_labelled)
    ).drop(["mainjrid_clean_labels_right", "ascwds_job_role_counts_right"])

    return df
