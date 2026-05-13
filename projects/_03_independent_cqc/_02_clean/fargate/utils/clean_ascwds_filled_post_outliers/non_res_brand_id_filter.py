import polars as pl
from datetime import date
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome


def non_res_brand_id_filter(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Nulls 'ascwds_filled_posts_dedup_clean' for non-residential
    locations associated with brand ID 'BD214' within a specified date range.

    This is done because the data submitted by the provider BD214 has weird
    jumps from March 2024 to May 2026, which we suspect are not valid. To avoid
    these potentially incorrect values skewing our analysis, we apply this
    filter to nullify the 'ascwds_filled_posts_dedup_clean' values for
    non-residential locations with the target brand ID within the specified date
    range.

    Rows are affected when:
    - 'care_home' equals 'not_care_home'
    - 'brand_id' equals 'BD214'
    - 'cqc_location_import_date' is strictly after 1st March 2024
    - 'cqc_location_import_date' is strictly before 1st May 2026

    For matching rows:
    - 'ascwds_filled_posts_dedup_clean' is set to null
    - the filtering rule is updated to 'brand_id_filter'

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
