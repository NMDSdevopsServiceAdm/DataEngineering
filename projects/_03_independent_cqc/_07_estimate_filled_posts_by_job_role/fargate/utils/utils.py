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

    df = df.with_columns(pl.lit(list_of_job_roles).alias("temp_potential_roles"))

    df = df.explode(pl.col("temp_potential_roles"))

    df = df.with_columns(
        pl.when(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == pl.col("temp_potential_roles")
        ).then(pl.col(IndCQC.ascwds_job_role_counts))
    ).drop(IndCQC.main_job_role_clean_labelled)

    df = df.rename({"temp_potential_roles": IndCQC.main_job_role_clean_labelled})

    df = df.select(
        [
            IndCQC.establishment_id,
            IndCQC.ascwds_worker_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.ascwds_job_role_counts,
        ]
    )

    print(df.collect().sample())

    return df


def join_worker_to_estimates_dataframe(
    estimated_filled_posts_lf: pl.LazyFrame,
    aggregated_job_roles_per_establishment_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Join the ASC-WDS job role count column from the aggregated worker file into the estimated filled post DataFrame.

    Join where establishment_id matches and ascwds_workplace_import_date == ascwds_worker_import_date.

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
