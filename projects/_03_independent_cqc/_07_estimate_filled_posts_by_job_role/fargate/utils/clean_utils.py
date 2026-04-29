import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EstimateFilledPostsSource


def nullify_job_role_count_when_source_not_ascwds(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Set job role counts to NULL when source is not ASCDWS.

    This is to ensure that we're only using ASCDWS job role data when ASCDWS data has
    been used for estimated filled posts.

    Nullify when the following conditions are NOT met:
    1. Source must be "ascwds_pir_merged"
    2. Estimates must equal the value after ASCWDS dedup_clean step.

    Args:
        lf (pl.LazyFrame): The estimated filled post by job role LazyFrame.

    Returns:
        pl.LazyFrame: Transformed LazyFrame with ASCDWS job role counts nullified.
    """
    source_is_ascwds = pl.col(IndCQC.estimate_filled_posts_source) == pl.lit(
        EstimateFilledPostsSource.ascwds_pir_merged
    )
    estimate_matches_ascwds = pl.col(IndCQC.estimate_filled_posts) == pl.col(
        IndCQC.ascwds_filled_posts_dedup_clean
    )

    return lf.with_columns(
        pl.when(source_is_ascwds & estimate_matches_ascwds)
        .then(IndCQC.ascwds_job_role_counts)
        .otherwise(None)
    )
