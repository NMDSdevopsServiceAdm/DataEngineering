from datetime import date

import polars as pl

from polars_utils.expressions import is_not_care_home
from polars_utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule


def non_res_brand_id_filter(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls 'ascwds_filled_posts_dedup_clean' for non-residential locations with a
    specific brand ID.

    This function identifies non-residential locations with the target brand ID
    (BD214) and nulls their 'ascwds_filled_posts_dedup_clean' values if they
    fall within the specified date range (between 1st March 2024 and 1st
    May 2026, exclusive). The filtering rule is updated to 'brand_id_filter'
    for affected rows.

    This is done because the data submitted by BD214 roughly halved in size in
    their 2024 submissions then returned to their original size in their 2026
    submission. This large drop off in filled posts didn't occur in their CQC, PIR
    or CT data so we have identified it as being a DQ issue.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame containing
            'ascwds_filled_posts_dedup_clean', 'care_home', 'brand_id', and
            'cqc_location_import_date' columns.
    Returns:
        pl.LazyFrame: A polars LazyFrame with 'ascwds_filled_posts_dedup_clean'
            nullified for affected non-residential locations and updated filtering
            rules.
    """
    target_brand_id = "BD214"
    start_date = date(2024, 3, 1)
    end_date = date(2026, 5, 1)

    lf = lf.with_columns(
        pl.when(
            is_not_care_home()
            & (pl.col(IndCQC.brand_id) == target_brand_id)
            & (pl.col(IndCQC.cqc_location_import_date) > start_date)
            & (pl.col(IndCQC.cqc_location_import_date) < end_date)
        )
        .then(None)
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .alias(IndCQC.ascwds_filled_posts_dedup_clean)
    )

    lf = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=IndCQC.ascwds_filtering_rule,
        raw_col_name=IndCQC.ascwds_filled_posts_dedup_clean,
        clean_col_name=IndCQC.ascwds_filled_posts_dedup_clean,
        populated_rule=AscwdsFilteringRule.populated,
        new_rule_name=AscwdsFilteringRule.brand_id_filter,
    )

    return lf
