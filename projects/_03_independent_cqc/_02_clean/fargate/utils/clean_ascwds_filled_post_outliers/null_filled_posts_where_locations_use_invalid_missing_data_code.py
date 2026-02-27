import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule

INVALID_MISSING_DATA_CODE = (
    999.0  # '999' is used elsewhere in ASCWDS to represent not known.
)


def null_filled_posts_where_locations_use_invalid_missing_data_code(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Null ascwds_filled_posts_dedup_clean values where locations have 999 in
    ascwds_filled_posts_dedup_clean.

    '999' is used elsewhere in ASCWDS to represent not known. Whilst that isn't
    the case for the variable totalstaff, upon investigation, all 999 entries do
    not look to be an accurate reflection of that location's staff so we are
    removing them for data quality.

    Args:
        lf(pl.LazyFrame): A polars LazyFrame with ascwds_filled_posts_dedup_clean.

    Returns:
        pl.LazyFrame: A polars LazyFrame with 999 ascwds_filled_posts_dedup_clean
            values nulled.
    """
    lf = lf.with_columns(
        pl.when(
            pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
            != pl.lit(INVALID_MISSING_DATA_CODE)
        )
        .then(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .otherwise(None)
        .alias(IndCQC.ascwds_filled_posts_dedup_clean)
    )
    lf = update_filtering_rule(
        lf,
        IndCQC.ascwds_filtering_rule,
        IndCQC.ascwds_filled_posts_dedup,
        IndCQC.ascwds_filled_posts_dedup_clean,
        AscwdsFilteringRule.populated,
        AscwdsFilteringRule.contained_invalid_missing_data_code,
    )
    return lf
