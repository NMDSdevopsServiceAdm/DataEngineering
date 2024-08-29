from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)

MISSING_DATA_CODE = 999.0


def null_filled_posts_where_locations_use_missing_data_code(df: DataFrame) -> DataFrame:
    """
    Null rows where locations have 999 in ascwds_filled_posts_dedup_clean.

    Args:
        df(DataFrame): A dataframe with ascwds_filled_posts_dedup_clean.

    Returns:
        DataFrame: A data frame with 999 values nulled.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            df[IndCQC.ascwds_filled_posts_dedup_clean] != F.lit(MISSING_DATA_CODE),
            df[IndCQC.ascwds_filled_posts_dedup_clean],
        ),
    )
    df = update_filtering_rule(df, AscwdsFilteringRule.contained_missing_data_code)
    return df
