import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTFilteringRule


def clean_longitudinal_outliers(
    lf: pl.LazyFrame,
    col_to_clean: str,
    cleaned_column_name: str,
    proportion_to_filter: float,
    care_home: bool,
) -> pl.LazyFrame:
    """
    Cleans longitudinal outliers from a numerical column in a LazyFrame.

    The function computes the group-wise median and absolute deviation,
    flags outliers based on the specified proportion to filter, and replaces
    outlier values with null. Additionally, updates filtering rules for care
    home or non-residential data.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the data to clean.
        col_to_clean (str): Column name containing numerical values to clean.
        cleaned_column_name (str): Name of the new column to store cleaned values.
        proportion_to_filter (float): Proportion of extreme values to consider as
            outliers.
        care_home (bool): If True, applies care home-specific filtering rules;
            otherwise, applies non-residential filtering rules.

    Returns:
        pl.LazyFrame: A new LazyFrame with outliers cleaned, updated filtering rules,
            and helper columns removed.
    """
    if care_home:
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule

    lf = compute_outlier_cutoff_and_clean(
        lf=lf,
        col_to_clean=col_to_clean,
        cleaned_column_name=cleaned_column_name,
        proportion_to_filter=proportion_to_filter,
    )

    lf = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=filter_rule_column_name,
        raw_col_name=col_to_clean,
        clean_col_name=cleaned_column_name,
        populated_rule=CTFilteringRule.populated,
        new_rule_name=CTFilteringRule.longitudinal_outliers,
    )

    return lf


def compute_outlier_cutoff_and_clean(
    lf: pl.LazyFrame,
    col_to_clean: str,
    cleaned_column_name: str,
    proportion_to_filter: float,
) -> pl.LazyFrame:
    """
    Computes the group-wise median, absolute deviation, global cutoff threshold,
    and applies outlier cleaning in a single LazyFrame pipeline.

    The cutoff is the (1 - proportion_to_filter) percentile of all absolute
    deviations from the group median. Values whose absolute deviation exceeds
    this cutoff are replaced with null in the cleaned column.

    Args:
        lf (pl.LazyFrame): Input LazyFrame.
        col_to_clean (str): Column containing numerical values to clean.
        cleaned_column_name (str): Name of the output cleaned column.
        proportion_to_filter (float): Proportion of extreme values to treat as
            outliers.

    Returns:
        pl.LazyFrame: LazyFrame with the cleaned column added and helper columns
            removed.
    """
    percentile = 1 - proportion_to_filter

    lf_median = lf.with_columns(
        pl.col(col_to_clean).median().over(IndCQC.location_id).alias("median_val")
    )
    lf_deviation = lf_median.with_columns(
        pl.when(pl.col(col_to_clean).is_not_null(), pl.col("median_val").is_not_null())
        .then((pl.col(col_to_clean) - pl.col("median_val")).abs())
        .otherwise(None)
        .alias("abs_diff")
    )

    cutoff = (
        lf_deviation.select(
            pl.col("abs_diff").quantile(percentile, interpolation="linear")
        )
        .collect()
        .item()
    )

    return lf_deviation.with_columns(
        pl.when(pl.col("abs_diff") > cutoff)
        .then(None)
        .otherwise(pl.col(col_to_clean))
        .alias(cleaned_column_name)
    ).drop(["abs_diff", "median_val"])
