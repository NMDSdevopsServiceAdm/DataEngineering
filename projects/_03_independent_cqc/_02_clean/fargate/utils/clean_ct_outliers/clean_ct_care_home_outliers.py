import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition import (
    clean_ct_values_after_consecutive_repetition,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.null_posts_per_bed_ratio_outliers import (
    null_posts_per_bed_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    add_filtering_rule_column,
    aggregate_values_to_provider_level,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTCareHomeFilteringRule


def clean_capacity_tracker_care_home_outliers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a clean version of Capacity Tracker care home data including a
    filtering rule column.

    This function first duplicates the original data into a cleaned column and
    then runs various functions designed to clean outlier values.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing `ct_care_home_total_employed`.

    Returns:
        pl.LazyFrame: A LazyFrame containing `ct_care_home_total_employed` and
            `ct_care_home_total_employed_cleaned`.
    """
    print("Cleaning Capacity Tracker care home data...")

    lf = lf.with_columns(
        pl.col(IndCQC.ct_care_home_total_employed).alias(
            IndCQC.ct_care_home_total_employed_cleaned
        )
    )
    lf = add_filtering_rule_column(
        lf,
        IndCQC.ct_care_home_filtering_rule,
        IndCQC.ct_care_home_total_employed_cleaned,
        CTCareHomeFilteringRule.populated,
        CTCareHomeFilteringRule.missing_data,
    )

    lf = aggregate_values_to_provider_level(lf, IndCQC.ct_care_home_total_employed)

    lf = null_posts_per_bed_outliers(lf)

    lf = clean_ct_values_after_consecutive_repetition(
        lf=lf,
        column_to_clean=IndCQC.ct_care_home_total_employed_cleaned,
        cleaned_column_name=IndCQC.ct_care_home_total_employed_cleaned,
        care_home=True,
        partitioning_column=IndCQC.location_id,
    )

    return lf
