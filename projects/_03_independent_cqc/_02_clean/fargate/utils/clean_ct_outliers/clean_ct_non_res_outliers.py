import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_longitudinal_outliers import (
    clean_longitudinal_outliers,
)
from projects._03_independent_cqc._02_clean.fargate.utils.clean_ct_outliers.clean_ct_repetition import (
    clean_ct_values_after_consecutive_repetition,
)
from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    add_filtering_rule_column,
    aggregate_values_to_provider_level,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTFilteringRule


def clean_capacity_tracker_non_res_outliers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a clean version of Capacity Tracker non residential data including a
    filtering rule column.

    This function first duplicates the original data into a cleaned column and
    then runs various functions designed to clean outlier values.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing
            `ct_non_res_care_workers_employed`.

    Returns:
        pl.LazyFrame: A LazyFrame containing `ct_non_res_care_workers_employed`
            and `ct_non_res_filtering_rule`.
    """
    print("Cleaning Capacity Tracker non-residential data...")

    lf = lf.with_columns(
        pl.col(IndCQC.ct_non_res_care_workers_employed).alias(
            IndCQC.ct_non_res_care_workers_employed_cleaned
        )
    )

    lf = add_filtering_rule_column(
        lf,
        IndCQC.ct_non_res_filtering_rule,
        IndCQC.ct_non_res_care_workers_employed_cleaned,
        CTFilteringRule.populated,
        CTFilteringRule.missing_data,
    )

    lf = aggregate_values_to_provider_level(lf, IndCQC.ct_non_res_care_workers_employed)

    lf = clean_ct_values_after_consecutive_repetition(
        lf=lf,
        column_to_clean=IndCQC.ct_non_res_care_workers_employed,
        cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
        care_home=False,
    )

    lf = clean_longitudinal_outliers(
        lf=lf,
        col_to_clean=IndCQC.ct_non_res_care_workers_employed,
        cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
        proportion_to_filter=0.001,
        care_home=False,
    )

    return lf
