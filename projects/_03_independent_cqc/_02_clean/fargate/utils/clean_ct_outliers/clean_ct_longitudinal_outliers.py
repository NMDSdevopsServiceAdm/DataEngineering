import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CTCareHomeFilteringRule,
    CTNonResFilteringRule,
)


def clean_longitudinal_outliers(
    lf: pl.LazyFrame,
    group_by_col: str,
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
        group_by_col (str): Column name to group by when computing medians and
            absolute deviation.
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
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = None
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.longitudinal_outliers

    lf = compute_outlier_cutoff_and_clean(
        lf=lf,
        group_by_col=group_by_col,
        col_to_clean=col_to_clean,
        cleaned_column_name=cleaned_column_name,
        proportion_to_filter=proportion_to_filter,
    )

    lf = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=filter_rule_column_name,
        raw_col_name=col_to_clean,
        clean_col_name=cleaned_column_name,
        populated_rule=populated_rule,
        new_rule_name=new_rule_name,
    )

    return lf


def compute_outlier_cutoff_and_clean(
    lf: pl.LazyFrame,
    group_by_col: str,
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
        group_by_col (str): Column to group by when computing the median.
        col_to_clean (str): Column containing numerical values to clean.
        cleaned_column_name (str): Name of the output cleaned column.
        proportion_to_filter (float): Proportion of extreme values to treat as
            outliers.

    Returns:
        pl.LazyFrame: LazyFrame with the cleaned column added and helper columns
            removed.
    """
    percentile = 1 - proportion_to_filter
    median_col = f"{col_to_clean}_median_val"
    abs_diff_col = f"{col_to_clean}_abs_diff"
    cutoff_col = f"{col_to_clean}_overall_abs_diff_cutoff"

    lf = lf.with_columns(
        pl.col(col_to_clean).median().over(group_by_col).alias(median_col)
    )

    lf = lf.with_columns(
        (pl.col(col_to_clean) - pl.col(median_col)).abs().alias(abs_diff_col)
    )

    lf = lf.with_columns(
        pl.col(abs_diff_col)
        .quantile(percentile, interpolation="linear")
        .first()
        .alias(cutoff_col)
    )

    lf = lf.with_columns(
        pl.when(pl.col(abs_diff_col) > pl.col(cutoff_col))
        .then(None)
        .otherwise(pl.col(col_to_clean))
        .alias(cleaned_column_name)
    ).drop([median_col, abs_diff_col, cutoff_col])

    return lf
