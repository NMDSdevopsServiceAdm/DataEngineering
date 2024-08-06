from pyspark.sql import DataFrame, functions as F

from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.null_care_home_filled_posts_per_bed_ratio_outliers import (
    null_care_home_filled_posts_per_bed_ratio_outliers,
)
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    add_filtering_rule_column,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


def null_ascwds_filled_post_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of 'ascwds_filled_posts_dedup' column and removes values identified as outliers.

    This function first duplicates 'ascwds_filled_posts_dedup' as 'ascwds_filled_posts_dedup_clean' and then runs
    various functions designed to clean ASCWDS filled post values, replacing outliers with null values.

    Args:
        df (DataFrame): A dataframe containing 'ascwds_filled_posts_dedup'.

    Returns:
        (DataFrame): A dataframe containing 'ascwds_filled_posts_dedup_clean' with outliers converted to nulls.
    """
    print("Replacing ascwds_filled_posts outliers with nulls...")

    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean, F.col(IndCQC.ascwds_filled_posts_dedup)
    )
    df = add_filtering_rule_column(df)
    filtered_df = null_care_home_filled_posts_per_bed_ratio_outliers(df)

    return filtered_df
