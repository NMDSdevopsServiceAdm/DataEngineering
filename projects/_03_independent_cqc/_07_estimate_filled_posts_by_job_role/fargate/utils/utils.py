import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
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
    to get a job role count per workplace. Then explodes a list of all potential job roles per row, and finally deduplicates
    rows on the exploded job role column by aggregating to get the sum of job role count per role.
    So if an establishment does not have particular role, then their count for that role is 0.
    All establishments end up with a row for all potential job roles.

    Args:
        lf (pl.LazyFrame): A dataframe containing cleaned ASC-WDS worker data.
        list_of_job_roles (list): A list of job roles in alphabetical order.

    Returns:
        pl.LazyFrame: The input dataframe with a count of rows for all potential job roles.
    """
    lf = lf.group_by(
        [
            pl.col(IndCQC.establishment_id),
            pl.col(IndCQC.ascwds_worker_import_date),
            pl.col(IndCQC.main_job_role_clean_labelled),
        ]
    ).len(name=IndCQC.ascwds_job_role_counts)

    lf = lf.with_columns(pl.lit(list_of_job_roles).alias("temp_potential_roles"))

    lf = lf.explode(pl.col("temp_potential_roles"))

    lf = lf.with_columns(
        pl.when(
            pl.col(IndCQC.main_job_role_clean_labelled)
            == pl.col("temp_potential_roles")
        ).then(pl.col(IndCQC.ascwds_job_role_counts))
    )

    lf = lf.drop(IndCQC.main_job_role_clean_labelled).rename(
        {"temp_potential_roles": IndCQC.main_job_role_clean_labelled}
    )

    new_columns_suffix = "_ascwds_counts"
    index = [
        pl.col(IndCQC.establishment_id),
        pl.col(IndCQC.ascwds_worker_import_date),
    ]
    on = pl.col(IndCQC.main_job_role_clean_labelled)
    values = pl.col(IndCQC.ascwds_job_role_counts)
    unique_values = list_of_job_roles
    aggregate_function = lambda col: col.sum()

    lf = lf.group_by(index).agg(
        aggregate_function(values.filter(on == value)).alias(value + new_columns_suffix)
        for value in unique_values
    )

    return lf


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
