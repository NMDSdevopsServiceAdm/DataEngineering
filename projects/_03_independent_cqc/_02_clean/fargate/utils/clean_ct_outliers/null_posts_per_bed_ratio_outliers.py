import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTCareHomeFilteringRule

MINIMUM_RATIO_CUTOFF: float = 0.66
MAXIMUM_RATIO_CUTOFF: float = 6.0


def null_posts_per_bed_outliers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls capacity tracker values where posts to bed ratio is out of bounds.

    Analysis of Capacity Tracker posts to bed ratio between April 2024 and
    September 2025 showed 95% of locations per month have a ratio between 0.66
    and 6.00.

    The filtering rule column is updated to 'beds_ratio_outlier' where outliers
    have been nulled.

    Args:
        lf(pl.LazyFrame): A LazyFrame with ct_care_home_posts_per_bed_ratio
            column.

    Returns:
        pl.LazyFrame: A LazyFrame with ct_care_home_total_employed_cleaned
            nulled if outside of ratio cutoffs.
    """
    posts_to_bed_ratio_col = pl.col(IndCQC.ct_care_home_posts_per_bed_ratio)
    value_is_null = posts_to_bed_ratio_col.is_null()
    value_is_within_bounds = (posts_to_bed_ratio_col > MINIMUM_RATIO_CUTOFF) & (
        posts_to_bed_ratio_col < MAXIMUM_RATIO_CUTOFF
    )
    lf = lf.with_columns(
        pl.when(value_is_null | value_is_within_bounds)
        .then(pl.col(IndCQC.ct_care_home_total_employed_cleaned))
        .otherwise(None)
        .alias(IndCQC.ct_care_home_total_employed_cleaned)
    )

    lf = update_filtering_rule(
        lf,
        IndCQC.ct_care_home_filtering_rule,
        IndCQC.ct_care_home_total_employed,
        IndCQC.ct_care_home_total_employed_cleaned,
        CTCareHomeFilteringRule.populated,
        CTCareHomeFilteringRule.beds_ratio_outlier,
    )

    return lf
