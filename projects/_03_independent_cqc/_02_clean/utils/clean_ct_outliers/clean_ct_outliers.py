from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_outliers(
    df: DataFrame,
    group_by_col: str,
    col_to_clean: str,
    proportion_to_filter: float,
    remove_whole_record: bool,
) -> DataFrame:

    df_median = compute_group_median(df, group_by_col, col_to_clean)
    df_deviation = compute_absolute_deviation(df_median, col_to_clean)
    df_mad = compute_mad(df_deviation, group_by_col)
    df_thresholds = compute_outlier_cutoff(df_mad, group_by_col, proportion_to_filter)

    df_flags = flag_outliers(df_thresholds)

    cleaned_df = apply_outlier_cleaning(df_flags, col_to_clean, remove_whole_record)

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
    remove_whole_record: bool,
) -> DataFrame:

    df = df.withColumn(
        "cleaned_value",
        F.when(F.col("outlier_flag"), None).otherwise(F.col(col_to_clean)),
    )
    if remove_whole_record:
        return df.filter(~F.col("outlier_flag"))
    else:
        return df
