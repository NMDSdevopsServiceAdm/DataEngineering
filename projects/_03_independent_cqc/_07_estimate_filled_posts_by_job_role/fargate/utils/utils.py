import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    EstimateFilledPostsSource,
)


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
        pl.LazyFrame: The estimated filled post LazyFrame with columns main_job_role_clean_labelled and ascwds_job_role_counts added.
    """

    merged_lf = estimated_filled_posts_lf.join(
        other=aggregated_job_roles_per_establishment_lf,
        left_on=[IndCQC.establishment_id, IndCQC.ascwds_workplace_import_date],
        right_on=[IndCQC.establishment_id, IndCQC.ascwds_worker_import_date],
        how="left",
    )

    return merged_lf


def nullify_job_role_count_when_source_not_ascwds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Set job role counts to NULL when source is not ASCDWS.

    This is to ensure that we're only using ASCDWS job role data when ASCDWS data has
    been used for estimated filled posts.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.

    Returns:
        pl.LazyFrame: Transformed LazyFrame with ASCDWS job role counts nullified.
    """
    return (
        lf
        # Nullify when conditions not met.
        # 1. Source must be "ascwds_pir_merged"
        # 2. Estimates must equal the value after dedup_clean step.
        .with_columns(
            # when combines comma-separated conditions with "&"
            pl.when(
                # First condition matches label in source column to a literal string.
                pl.col(IndCQC.estimate_filled_posts_source)
                == pl.lit(EstimateFilledPostsSource.ascwds_pir_merged),
                # Second condition compares float values.
                pl.col(IndCQC.estimate_filled_posts)
                == pl.col(IndCQC.ascwds_filled_posts_dedup_clean),
            ).then(IndCQC.ascwds_job_role_counts)
        )
    )


def get_percentage_share(values: str) -> pl.Expr:
    """Calculate the percentage share of a column across all values."""
    return pl.col(values) / pl.col(values).sum()
