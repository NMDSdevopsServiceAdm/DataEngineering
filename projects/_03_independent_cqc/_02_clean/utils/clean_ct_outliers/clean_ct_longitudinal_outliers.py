from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    update_filtering_rule,
)
from projects.utils.utils.utils import calculate_new_column

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CTCareHomeFilteringRule,
    CTNonResFilteringRule,
)


def clean_longitudinal_outliers(
    df: DataFrame,
    group_by_col: str,
    col_to_clean: str,
    cleaned_column_name: str,
    proportion_to_filter: float,
    care_home: bool,
) -> DataFrame:
    """
    Cleans longitudinal outliers from a numerical column in a DataFrame.

    The function computes the group-wise median and absolute difference,
    flags outliers based on the specified proportion to filter, and replaces
    outlier values with null. Additionally, updates filtering
    rules for care home or non-residential data.

    Args:
        df (DataFrame): Input DataFrame containing the data to clean.
        group_by_col (str): Column name to group by when computing medians and Absolute difference.
        col_to_clean (str): Column name containing numerical values to clean.
        cleaned_column_name (str): Name of the new column to store cleaned values.
        proportion_to_filter (float): Proportion of extreme values to consider as outliers.
        care_home (bool): If True, applies care home-specific filtering rules;
            otherwise, applies non-residential filtering rules.

    Returns:
        DataFrame: A new PySpark DataFrame with outliers cleaned, updated filtering rules,
            and helper columns removed.
    """
    df_median = compute_group_median(df, group_by_col, col_to_clean)
    df_deviation = calculate_new_column(
        df_median,
        f"{col_to_clean}_abs_diff",
        col_to_clean,
        "absolute difference",
        f"{col_to_clean}_median_val",
    )
    df_thresholds = compute_outlier_cutoff(
        df_deviation, proportion_to_filter, col_to_clean
    )
    cleaned_df = apply_outlier_cleaning(
        df_thresholds, col_to_clean, cleaned_column_name
    )

    if care_home:
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = None
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.longitudinal_outliers

    cleaned_df = update_filtering_rule(
        cleaned_df,
        filter_rule_column_name,
        col_to_clean,
        cleaned_column_name,
        populated_rule,
        new_rule_name,
    )

    cleaned_df = cleaned_df.drop(
        f"{col_to_clean}_median_val",
        f"{col_to_clean}_abs_diff",
        f"{col_to_clean}_overall_abs_diff_cutoff",
    )

    return cleaned_df


def compute_group_median(df: DataFrame, group_col: str, col_to_clean: str) -> DataFrame:
    """
    Computes the median value of a numerical column within each group.

    Args:
        df (DataFrame): Input DataFrame.
        group_col (str): Column name to group by.
        col_to_clean (str): Column for which to compute the median.

    Returns:
        DataFrame: Original DataFrame joined with a new column 'median_val' containing
        the group-wise median.
    """

    w = Window.partitionBy(group_col)
    df = df.withColumn(
        f"{col_to_clean}_median_val",
        F.percentile(col_to_clean, 0.5).over(w),
    )
    return df


def compute_absolute_deviation(df: DataFrame, col_to_clean: str) -> DataFrame:
    """
    Computes the absolute deviation of each value from the group median.

    Args:
        df (DataFrame): DataFrame containing a 'median_val' column.
        col_to_clean (str): Column for which to compute absolute deviation.

    Returns:
        DataFrame: DataFrame with a new column 'abs_diff' representing the absolute
        deviation of each value from the median.
    """
    return df.withColumn(
        f"{col_to_clean}_abs_diff",
        F.abs(F.col(col_to_clean) - F.col(f"{col_to_clean}_median_val")),
    )


def compute_outlier_cutoff(
    df: DataFrame,
    proportion_to_filter: float,
    col_to_clean: str,
) -> DataFrame:
    """
    Computes the threshold value beyond which a data point is considered an outlier
    for each group, based on the specified proportion of extreme values.

    Args:
        df (DataFrame): DataFrame containing an 'abs_diff' column.
        proportion_to_filter (float): Proportion of extreme values to consider as outliers.
        col_to_clean (str): Column to use to get outlier cutoff.

    Returns:
        DataFrame: Original DataFrame joined with a new column 'overall_abs_diff_cutoff'
        containing the outlier threshold for each group.
    """
    percentile = 1 - proportion_to_filter

    overall_abs_diff_cutoff = df.agg(
        F.percentile(f"{col_to_clean}_abs_diff", percentile).alias(
            "overall_abs_diff_cutoff"
        )
    ).first()["overall_abs_diff_cutoff"]
    df = df.withColumn(
        f"{col_to_clean}_overall_abs_diff_cutoff",
        F.lit(overall_abs_diff_cutoff),
    )
    return df


def apply_outlier_cleaning(
    df: DataFrame,
    col_to_clean: str,
    cleaned_column_name: str,
) -> DataFrame:
    """
    Removes outlier values from a numeric column by setting them to null when the
    abs_diff exceeds the overall_abs_diff_cutoff for any record.

    Args:
        df (DataFrame): DataFrame containing 'abs_diff' and 'overall_abs_diff_cutoff'.
        col_to_clean (str): Column to clean.
        cleaned_column_name (str): Name of the new column to store cleaned values.

    Returns:
        DataFrame: DataFrame with outliers cleaned by null replacement.
    """
    df = df.withColumn(
        cleaned_column_name,
        F.when(
            F.col(f"{col_to_clean}_abs_diff")
            > F.col(f"{col_to_clean}_overall_abs_diff_cutoff"),
            None,
        ).otherwise(F.col(col_to_clean)),
    )
    return df
