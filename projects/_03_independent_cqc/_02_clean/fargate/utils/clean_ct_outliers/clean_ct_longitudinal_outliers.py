import polars as pl

from projects._03_independent_cqc._02_clean.fargate.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTFilteringRule


def clean_longitudinal_outliers(
    lf: pl.LazyFrame,
    column_to_clean: str,
    cleaned_column_name: str,
    proportion_to_filter: float,
    care_home: bool,
) -> pl.LazyFrame:
    """
    Cleans longitudinal outliers from a numerical column in a LazyFrame.

    Warning: This function will return null values where the filtering
    rule is not "populated", as well as outlying values.

    The function computes the group-wise median and absolute deviation,
    on rows where the filtering rule is "populated".
    The cutoff is then determined by the value at the given percentile.
    Values greater than the cutoff are nulled.
    The filtering rule value is then updated.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the data to clean.
        column_to_clean (str): Column name containing numerical values to clean.
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

    percentile = 1 - proportion_to_filter

    median_expr = (
        pl.col(column_to_clean)
        .median()
        .over([IndCQC.location_id, filter_rule_column_name])
    )

    abs_diff_expr = pl.when(
        pl.col(filter_rule_column_name) == CTFilteringRule.populated
    ).then((pl.col(column_to_clean) - median_expr).abs())

    cutoff_expr = abs_diff_expr.quantile(percentile, interpolation="linear").first()

    lf = lf.with_columns(
        pl.when(
            (pl.col(filter_rule_column_name) == CTFilteringRule.populated)
            & (abs_diff_expr <= cutoff_expr)
        )
        .then(pl.col(column_to_clean))
        .otherwise(None)
        .alias(cleaned_column_name)
    )

    lf = update_filtering_rule(
        lf=lf,
        filter_rule_col_name=filter_rule_column_name,
        raw_col_name=column_to_clean,
        clean_col_name=cleaned_column_name,
        populated_rule=CTFilteringRule.populated,
        new_rule_name=CTFilteringRule.longitudinal_outliers,
    )

    return lf
