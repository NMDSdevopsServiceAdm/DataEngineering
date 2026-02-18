import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_filled_posts_where_locations_use_invalid_missing_data_code import (
    null_filled_posts_where_locations_use_invalid_missing_data_code,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.null_grouped_providers import (
    null_grouped_providers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.winsorize_care_home_filled_posts_per_bed_ratio_outliers import (
    winsorize_care_home_filled_posts_per_bed_ratio_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    add_filtering_rule_column,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import AscwdsFilteringRule


def clean_ascwds_filled_post_outliers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a clean version of 'ascwds_filled_posts_dedup' column.

    This function first duplicates 'ascwds_filled_posts_dedup' as 'ascwds_filled_posts_dedup_clean' and then runs
    various functions designed to clean ASCWDS filled post values.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame containing 'ascwds_filled_posts_dedup'.

    Returns:
        pl.LazyFrame: A polars LazyFrame containing 'ascwds_filled_posts_dedup_clean'.
    """
    print("Cleaning ascwds_filled_posts_dedup...")

    lf = lf.with_columns(
        pl.col(IndCQC.ascwds_filled_posts_dedup).alias(
            IndCQC.ascwds_filled_posts_dedup_clean
        )
    )
    lf = add_filtering_rule_column(
        lf,
        IndCQC.ascwds_filtering_rule,
        IndCQC.ascwds_filled_posts_dedup_clean,
        AscwdsFilteringRule.populated,
        AscwdsFilteringRule.missing_data,
    )

    lf = null_filled_posts_where_locations_use_invalid_missing_data_code(lf)
    lf = null_grouped_providers(lf)
    lf = winsorize_care_home_filled_posts_per_bed_ratio_outliers(lf)

    return lf
