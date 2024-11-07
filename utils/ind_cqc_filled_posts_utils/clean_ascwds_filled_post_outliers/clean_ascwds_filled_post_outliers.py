from pyspark.sql import DataFrame, functions as F

from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    add_filtering_rule_column,
)
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.winsorize_care_home_filled_posts_per_bed_ratio_outliers import (
    winsorize_care_home_filled_posts_per_bed_ratio_outliers,
)
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.null_filled_posts_where_locations_use_invalid_missing_data_code import (
    null_filled_posts_where_locations_use_invalid_missing_data_code,
)
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.null_grouped_providers import (
    null_grouped_providers,
)

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)


def clean_ascwds_filled_post_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of 'ascwds_filled_posts_dedup' column.

    This function first duplicates 'ascwds_filled_posts_dedup' as 'ascwds_filled_posts_dedup_clean' and then runs
    various functions designed to clean ASCWDS filled post values.

    Args:
        df (DataFrame): A dataframe containing 'ascwds_filled_posts_dedup'.

    Returns:
        DataFrame: A dataframe containing 'ascwds_filled_posts_dedup_clean'.
    """
    print("Cleaning ascwds_filled_posts_dedup...")

    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean, F.col(IndCQC.ascwds_filled_posts_dedup)
    )
    df = add_filtering_rule_column(df)
    df = null_filled_posts_where_locations_use_invalid_missing_data_code(df)
    df = null_grouped_providers(df)
    filtered_df = winsorize_care_home_filled_posts_per_bed_ratio_outliers(df)

    return filtered_df
