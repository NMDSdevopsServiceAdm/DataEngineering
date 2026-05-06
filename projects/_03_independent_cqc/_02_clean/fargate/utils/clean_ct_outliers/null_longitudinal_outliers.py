import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTFilteringRule


def null_longitudinal_outliers(
    lf: pl.LazyFrame,
    column_to_clean: str,
    proportion_to_filter: float,
    filter_rule_column_name: str,
) -> pl.LazyFrame:
    """
    Cleans longitudinal outliers from a numerical column in a LazyFrame.

    The function computes the group-wise median and absolute deviation,
    on rows where the filtering rule is "populated".
    The cutoff is then determined by the value at the given percentile.
    Values greater than the cutoff are nulled.
    The filtering rule value is then updated.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the data to clean.
        column_to_clean (str): Column name containing numerical values to clean.
        proportion_to_filter (float): Proportion of extreme values to consider as
            outliers.
        filter_rule_column_name (str): Column name for the filtering rule.

    Returns:
        pl.LazyFrame: A new LazyFrame with outliers cleaned, updated filtering rules,
            and helper columns removed.
    """
    percentile = 1 - proportion_to_filter

    median_expr = pl.col(column_to_clean).median().over([IndCQC.location_id])

    abs_diff_expr = (pl.col(column_to_clean) - median_expr).abs()

    cutoff_expr = abs_diff_expr.quantile(percentile, interpolation="linear").first()
    cutoff_expr_01 = abs_diff_expr.quantile(0.99, interpolation="linear").first()
    cutoff_expr_025 = abs_diff_expr.quantile(0.975, interpolation="linear").first()
    cutoff_expr_005 = abs_diff_expr.quantile(0.995, interpolation="linear").first()

    lf = lf.with_columns(
        median_expr.alias(f"{column_to_clean}_median"),
        abs_diff_expr.alias(f"{column_to_clean}_abs_diff_from_median"),
        cutoff_expr.alias(f"{column_to_clean}_001_cutoff"),
        cutoff_expr_01.alias(f"{column_to_clean}_01_cutoff"),
        cutoff_expr_025.alias(f"{column_to_clean}_025_cutoff"),
        cutoff_expr_005.alias(f"{column_to_clean}_005_cutoff"),
        pl.when(
            (pl.col(column_to_clean).is_not_null()) & (abs_diff_expr <= cutoff_expr)
        )
        .then(pl.col(column_to_clean))
        .otherwise(None)
        .alias(column_to_clean),
    )

    lf = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=filter_rule_column_name,
        raw_col_name=column_to_clean,
        clean_col_name=column_to_clean,
        populated_rule=CTFilteringRule.populated,
        new_rule_name=CTFilteringRule.longitudinal_outliers,
    )

    return lf
