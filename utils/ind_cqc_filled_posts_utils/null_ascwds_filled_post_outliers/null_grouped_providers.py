from pyspark.sql import DataFrame, functions as F

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


def null_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple locations, all their ascwds is under one location.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with grouped providers' data nulled.
    """
    df = null_care_home_grouped_providers(df)
    df = null_non_residential_grouped_providers(df)
    return df


def null_care_home_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple locations, all their ascwds is under one care home location.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with grouped providers' care home data nulled.
    """
    df = update_filtering_rule(
        df, rule_name=AscwdsFilteringRule.care_home_location_was_grouped_provider
    )
    return df


def null_non_residential_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple locations, all their ascwds is under one non-residential location.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with grouped providers' non-residential data nulled.
    """
    # TODO: Design filter for non-res grouped providers.
    return df
