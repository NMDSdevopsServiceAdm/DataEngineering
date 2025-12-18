from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    update_filtering_rule,
)
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

    The function computes the group-wise median and median absolute deviation (MAD),
    flags outliers based on the specified proportion to filter, and either replaces
    outlier values with null or removes entire records. Additionally, updates filtering
    rules for care home or non-residential data.

    Args:
        df (DataFrame): Input DataFrame containing the data to clean.
        group_by_col (str): Column name to group by when computing medians and MADs.
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
    df_deviation = compute_absolute_deviation(df_median, col_to_clean)
    df_mad = compute_mad(df_deviation, group_by_col, col_to_clean)
    df_thresholds = compute_outlier_cutoff(
        df_mad, group_by_col, proportion_to_filter, col_to_clean
    )
    large_location_cutoff = compute_large_location_cutoff(
        df_thresholds, 0.95, col_to_clean
    )
    df_flags = flag_outliers(df_thresholds, col_to_clean)
    df_flags = flag_large_locations(
        df_flags, group_by_col, col_to_clean, large_location_cutoff
    )
    cleaned_df = apply_outlier_cleaning(df_flags, col_to_clean, cleaned_column_name)

    if care_home:
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = CTCareHomeFilteringRule.longitudinal_outliers_total_posts
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.longitudinal_outliers_total_posts

    cleaned_df = update_filtering_rule(
        cleaned_df,
        filter_rule_column_name,
        col_to_clean,
        cleaned_column_name,
        populated_rule,
        new_rule_name,
    )

    # cleaned_df = cleaned_df.drop(
    #     f"{col_to_clean}_median_val",
    #     f"{col_to_clean}_mad",
    #     f"{col_to_clean}_mad_abs_diff",
    #     f"{col_to_clean}_abs_diff",
    #     f"{col_to_clean}_abs_diff_cutoff",
    #     f"{col_to_clean}_outlier_flag",
    #     f"{col_to_clean}_large_location_flag",
    # )

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
        F.percentile_approx(col_to_clean, 0.5).over(w),
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


def compute_mad(df: DataFrame, group_by_col: str, col_to_clean: str) -> DataFrame:
    """
    Computes the median absolute deviation (MAD) of a column within each group.

    Args:
        df (DataFrame): DataFrame containing an 'abs_diff' column.
        group_by_col (str): Column to group by when computing MAD.
        col_to_clean (str): Column for which MAD is calculated

    Returns:
        DataFrame: Original DataFrame joined with a new column 'mad' containing the
        group-wise median absolute deviation.
    """
    w = Window.partitionBy(group_by_col)

    df = df.withColumn(
        f"{col_to_clean}_mad",
        F.percentile_approx(f"{col_to_clean}_abs_diff", 0.5).over(w),
    )
    return df.withColumn(
        f"{col_to_clean}_mad_abs_diff",
        F.abs(F.col(col_to_clean) - F.col(f"{col_to_clean}_mad")),
    )


def compute_outlier_cutoff(
    df: DataFrame,
    group_by_col: str,
    proportion_to_filter: float,
    col_to_clean: str,
) -> DataFrame:
    """
    Computes the threshold value beyond which a data point is considered an outlier
    for each group, based on the specified proportion of extreme values.

    Args:
        df (DataFrame): DataFrame containing an 'abs_diff' column.
        group_by_col (str): Column to group by when computing cutoff thresholds.
        proportion_to_filter (float): Proportion of extreme values to consider as outliers.
        col_to_clean (str): Column to use to get outlier cutoff.

    Returns:
        DataFrame: Original DataFrame joined with a new column 'abs_diff_cutoff'
        containing the outlier threshold for each group.
    """
    percentile = 1 - proportion_to_filter
    w = Window.partitionBy(group_by_col)

    df = df.withColumn(
        f"{col_to_clean}_abs_diff_cutoff",
        F.percentile_approx(f"{col_to_clean}_abs_diff", percentile).over(w),
    )
    return df


def flag_outliers(df: DataFrame, col_to_clean: str) -> DataFrame:
    """
    Flags outlier records based on whether the absolute deviation exceeds the
    group-specific cutoff.

    Args:
        df (DataFrame): DataFrame containing 'abs_diff' and 'abs_diff_cutoff' columns.
        col_to_clean (str): Column for which outliers are to be flagged.

    Returns:
        DataFrame: DataFrame with a new boolean column 'outlier_flag' where True indicates
        an outlier.
    """
    return df.withColumn(
        f"{col_to_clean}_outlier_flag",
        F.col(f"{col_to_clean}_abs_diff") > F.col(f"{col_to_clean}_abs_diff_cutoff"),
    )


def compute_large_location_cutoff(
    df: DataFrame,
    large_location_threshold_percentile: float,
    col_to_clean: str,
) -> float:
    """
    Computes the threshold at which a location is catageorised as a large location compared to other locaitons.

    Args:
        df (DataFrame): DataFrame containing the column to clean.
        large_location_threshold_percentile (float): Percentile above which locations are considered large.
        col_to_clean (str): Column to use to calculate large location cutoff.

    Returns:
        float: The number of posts above which a location is considered large.
    """
    large_location_threshold_abs = df.agg(
        F.percentile_approx(col_to_clean, large_location_threshold_percentile).alias(
            "large_location_threshold_abs"
        )
    ).first()["large_location_threshold_abs"]
    return large_location_threshold_abs


def flag_large_locations(
    df: DataFrame, group_by_col: str, col_to_clean: str, large_location_cutoff: float
) -> DataFrame:
    """
    Flags locations that have ever exceed the large location cutoff in their history

    Args:
        df (DataFrame): DataFrame containing column to clean.
        group_by_col (str): Column to be used for grouping.
        col_to_clean (str): Column for which large locations are to be flagged.
        large_location_cutoff(float): The number of posts above which a location is considered large.

    Returns:
        DataFrame: DataFrame with a new boolean column 'large_location_flag' where True indicates
        a location has been large at some point in its history.
    """
    w = Window.partitionBy(group_by_col)
    df = df.withColumn(
        f"{col_to_clean}_large_location_flag",
        F.max(F.col(col_to_clean)).over(w) > large_location_cutoff,
    )
    return df


def apply_outlier_cleaning(
    df: DataFrame,
    col_to_clean: str,
    cleaned_column_name: str,
) -> DataFrame:
    """
    Cleans outlier values in a numerical column based on an 'outlier_flag' column.

    Args:
        df (DataFrame): DataFrame containing 'outlier_flag' and 'large_location_flag'.
        col_to_clean (str): Column to clean.
        cleaned_column_name (str): Name of the new column to store cleaned values.

    Returns:
        DataFrame: DataFrame with outliers cleaned by null replacement.
    """
    df = df.withColumn(
        cleaned_column_name,
        F.when(
            F.col(f"{col_to_clean}_outlier_flag")
            & F.col(f"{col_to_clean}_large_location_flag"),
            None,
        ).otherwise(F.col(col_to_clean)),
    )
    return df
