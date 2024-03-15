from pyspark.sql import DataFrame

from utils.ind_cqc_filled_posts_utils.filter_ascwds_filled_posts.remove_care_home_filled_posts_per_bed_ratio_outliers import (
    remove_care_home_filled_posts_per_bed_ratio_outliers,
)


def null_ascwds_filled_post_outliers(input_df: DataFrame) -> DataFrame:
    print("Removing ascwds_filled_posts outliers...")

    filtered_df = remove_care_home_filled_posts_per_bed_ratio_outliers(input_df)

    return filtered_df
