import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def filter_out_zero_job_role_filled_posts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Removes rows where the job role has zero estimated filled posts.

    Ticket 2110 spike: employment status shares are meaningless for a job role
    with no filled posts, so these rows (~48% of the reduced dataset) are dropped
    before imputation. Null estimates are kept explicitly, because a plain
    `!= 0` filter would also drop them.

    Args:
        lf (pl.LazyFrame): Data containing `estimate_filled_posts_by_job_role`.

    Returns:
        pl.LazyFrame: `lf` without the zero-filled-post rows.
    """
    metric = pl.col(IndCQC.estimate_filled_posts_by_job_role)
    return lf.filter(metric.is_null() | (metric != 0))
