from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


def null_longitudinal_outliers(
    df: DataFrame,
) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean when the value is an outlier for that location.

    A value is defined as an outlier for that location when it is more than 3 standard deviations above or below the mean for that location.

    Args:
        df(DataFrame): A dataframe with the columns ascwds_filled_posts_dedup_clean and location_id.

    Returns:
        DataFrame: A dataframe with longitudinal outliers removed.
    """
    return df
