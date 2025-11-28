from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CTCareHomeFilteringRule,
    CTNonResFilteringRule,
)


def clean_random_spikes(
    df: DataFrame,
    group_by_col: str,
    col_to_clean: str,
    cleaned_column_name: str,
    proportion_to_filter: float,
    remove_whole_record: bool,
    care_home: bool,
) -> DataFrame:

    df_median = compute_group_median(df, group_by_col, col_to_clean)
    df_deviation = compute_absolute_deviation(df_median, col_to_clean)
    df_mad = compute_mad(df_deviation, group_by_col)
    df_thresholds = compute_outlier_cutoff(df_mad, group_by_col, proportion_to_filter)

    df_flags = flag_outliers(df_thresholds)

    cleaned_df = apply_outlier_cleaning(
        df_flags, col_to_clean, cleaned_column_name, remove_whole_record
    )

    if care_home:
        filter_rule_column_name = IndCQC.ct_care_home_filtering_rule
        populated_rule = CTCareHomeFilteringRule.populated
        new_rule_name = CTCareHomeFilteringRule.random_spikes_total_posts
    else:
        filter_rule_column_name = IndCQC.ct_non_res_filtering_rule
        populated_rule = CTNonResFilteringRule.populated
        new_rule_name = CTNonResFilteringRule.random_spikes_total_posts

    cleaned_df = update_filtering_rule(
        cleaned_df,
        filter_rule_column_name,
        col_to_clean,
        cleaned_column_name,
        populated_rule,
        new_rule_name,
    )
    cleaned_df = cleaned_df.drop(
        "median_val",
        "abs_diff",
        "mad",
        "abs_diff_cutoff",
        "outlier_flag",
    )
    return cleaned_df


def compute_group_median(df: DataFrame, group_col: str, col_to_clean: str) -> DataFrame:

    median_df = df.groupBy(group_col).agg(
        F.expr(f"percentile({col_to_clean}, array(0.5))")[0].alias("median_val")
    )

    return df.join(median_df, group_col, "left")


def compute_absolute_deviation(df: DataFrame, col_to_clean: str) -> DataFrame:

    return df.withColumn("abs_diff", F.abs(F.col(col_to_clean) - F.col("median_val")))


def compute_mad(df: DataFrame, group_by_col: str) -> DataFrame:

    mad_df = df.groupBy(group_by_col).agg(
        F.expr("percentile(abs_diff, array(0.5))")[0].alias("mad")
    )

    return df.join(mad_df, group_by_col, "left")


def compute_outlier_cutoff(
    df: DataFrame,
    group_by_col: str,
    proportion_to_filter: float,
) -> DataFrame:

    percentile = 1 - proportion_to_filter

    cutoff_df = df.groupBy(group_by_col).agg(
        F.expr(f"percentile(abs_diff, array({percentile}))")[0].alias("abs_diff_cutoff")
    )

    return df.join(cutoff_df, group_by_col, "left")


def flag_outliers(df: DataFrame) -> DataFrame:

    return df.withColumn("outlier_flag", F.col("abs_diff") > F.col("abs_diff_cutoff"))


def apply_outlier_cleaning(
    df: DataFrame,
    col_to_clean: str,
    cleaned_column_name: str,
    remove_whole_record: bool,
) -> DataFrame:

    df = df.withColumn(
        cleaned_column_name,
        F.when(F.col("outlier_flag"), None).otherwise(F.col(col_to_clean)),
    )
    if remove_whole_record:
        return df.filter(~F.col("outlier_flag"))
    else:
        return df
