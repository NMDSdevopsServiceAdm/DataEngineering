from dataclasses import dataclass

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    ASCWDSFilledPostsSource as Source,
)


@dataclass
class CalculationConstants:
    MAX_ABSOLUTE_DIFFERENCE: int = 5
    MAX_PERCENTAGE_DIFFERENCE: float = 0.1
    MIN_PERMITTED_POSTS: int = 3


total_staff_expr = pl.col(IndCQC.total_staff_bounded)
worker_records_expr = pl.col(IndCQC.worker_records_bounded)

ASCWDS_POSTS_COL = IndCQC.ascwds_filled_posts
ASCWDS_SOURCE_COL = IndCQC.ascwds_filled_posts_source


def calculate_ascwds_filled_posts(input_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Derive ASC-WDS filled posts from total staff and worker record counts.

    Applies a sequence of reconciliation rules to derive a robust
    `ascwds_filled_posts` estimate and records the source in
    `ascwds_filled_posts_source`.

    Rules are applied in priority order:
        1. Use worker records when total staff and worker records match exactly
        2. Use the average when both values are sufficiently close

    Existing calculated values are preserved once populated by an earlier rule.

    Args:
        input_lf (pl.LazyFrame): Input LazyFrame containing total staff and
            worker record columns.

    Returns:
        pl.LazyFrame: LazyFrame with derived filled posts and source columns
            added.
    """
    print("Calculating ascwds_filled_posts...")

    input_lf = input_lf.with_columns(
        [
            pl.lit(None).cast(pl.Int64).alias(ASCWDS_POSTS_COL),
            pl.lit(None).cast(pl.Utf8).alias(ASCWDS_SOURCE_COL),
        ]
    )

    input_lf = populate_from_exact_staff_match(input_lf)

    return populate_from_similar_staff_counts(input_lf)


def populate_from_exact_staff_match(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Populate filled posts where total staff equals worker records.

    When both staff measures match exactly and meet the minimum permitted
    threshold, the shared value is considered reliable and is used directly as
    the filled posts estimate.

    The source column is updated only for rows populated by this rule.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with exact-match rows populated.
    """
    lf = lf.with_columns(
        pl.when(
            pl.col(ASCWDS_POSTS_COL).is_null()
            & total_staff_expr.is_not_null()
            & (total_staff_expr == worker_records_expr)
            & (total_staff_expr >= CalculationConstants.MIN_PERMITTED_POSTS)
        )
        .then(worker_records_expr)
        .otherwise(pl.col(ASCWDS_POSTS_COL))
        .alias(ASCWDS_POSTS_COL)
    )

    return set_source_for_newly_populated_rows(
        lf, Source.worker_records_and_total_staff
    )


def populate_from_similar_staff_counts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Populate filled posts using the mean of similar staff counts.

    When bounded total staff and worker records are both above the minimum
    threshold and sufficiently close, in either absolute difference or
    percentage difference, then the average value is used as the filled posts
    value.

    The source column is updated only for rows populated by this rule.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.

    Returns:
        pl.LazyFrame: LazyFrame with similar-count rows populated.
    """
    abs_diff = (total_staff_expr - worker_records_expr).abs()
    avg_of_cols = (total_staff_expr + worker_records_expr) / 2
    perc_diff = abs_diff / avg_of_cols

    abs_diff_within_range = abs_diff < CalculationConstants.MAX_ABSOLUTE_DIFFERENCE
    per_diff_within_range = perc_diff < CalculationConstants.MAX_PERCENTAGE_DIFFERENCE

    lf = lf.with_columns(
        pl.when(
            (
                pl.col(ASCWDS_POSTS_COL).is_null()
                & total_staff_expr.is_not_null()
                & worker_records_expr.is_not_null()
                & (total_staff_expr >= CalculationConstants.MIN_PERMITTED_POSTS)
                & (worker_records_expr >= CalculationConstants.MIN_PERMITTED_POSTS)
                & (abs_diff_within_range | per_diff_within_range)
            )
        )
        .then(avg_of_cols)
        .otherwise(pl.col(ASCWDS_POSTS_COL))
        .alias(ASCWDS_POSTS_COL)
    )

    return set_source_for_newly_populated_rows(
        lf, Source.average_of_total_staff_and_worker_records
    )


def set_source_for_newly_populated_rows(
    lf: pl.LazyFrame, source_description: str
) -> pl.LazyFrame:
    """
    Records the source for rows where filled posts were newly populated.

    Updates the source column only where:
        - `ascwds_filled_posts` is populated
        - `ascwds_filled_posts_source` is still null

    Args:
        lf (pl.LazyFrame): Input LazyFrame.
        source_description (str): Provenance label for the applied rule.

    Returns:
        pl.LazyFrame: LazyFrame with source values updated.
    """
    return lf.with_columns(
        pl.when(
            pl.col(ASCWDS_POSTS_COL).is_not_null() & pl.col(ASCWDS_SOURCE_COL).is_null()
        )
        .then(pl.lit(source_description))
        .otherwise(pl.col(ASCWDS_SOURCE_COL))
        .alias(ASCWDS_SOURCE_COL)
    )
