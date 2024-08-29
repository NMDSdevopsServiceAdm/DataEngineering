from pyspark.sql import DataFrame, functions as F

from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


def null_filled_posts_where_locations_use_missing_data_code(df: DataFrame) -> DataFrame:
    """
    Null rows where locations have 999 in ascwds_filled_posts_dedup_clean.

    Args:
        df(DataFrame): A dataframe with ascwds_filled_posts_dedup_clean.

    Returns:
        DataFrame: A data frame with 999 values nulled.
    """
    return df
