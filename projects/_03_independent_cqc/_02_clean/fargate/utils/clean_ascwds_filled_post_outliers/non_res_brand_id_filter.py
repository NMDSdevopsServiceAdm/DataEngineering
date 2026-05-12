import polars as pl

import polars_utils.cleaning_utils as pUtils
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome


def non_res_brand_id_filter(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls 'ascwds_filled_posts_dedup_clean' for non-residential locations with a
    specific brand ID.

    This function identifies non-residential locations with the target brand ID
    (BD214) and nulls their 'ascwds_filled_posts_dedup_clean' values if they
    fall within the specified date range (after 1st March 2024 and before 1st
    May 2026). The filtering rule is updated to 'brand_id_filter' for affected
    rows.

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
    start_date = pl.Date("2024-03-01")
    end_date = pl.Date("2026-05-01")

    lf = lf.with_columns(
        pl.when(
            (pl.col(IndCQC.care_home) == CareHome.not_care_home)
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
