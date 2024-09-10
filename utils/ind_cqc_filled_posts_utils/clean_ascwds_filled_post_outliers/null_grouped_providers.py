from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


@dataclass
class NullGroupedProvidersConfig:
    """Configuration values for defining grouped providers"""

    max_locations_at_provider = 2
    single_location_in_ascwds = 1
    no_locations_with_data = 0


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
    df = calculate_data_for_grouped_provider_identification(df)
    df = null_values_which_meet_the_grouped_provider_criteria(df)
    # df = df.drop(
    #     *[
    #         IndCQC.locations_at_provider,
    #         IndCQC.locations_in_ascwds_at_provider,
    #         IndCQC.locations_in_ascwds_with_data_at_provider,
    #         IndCQC.number_of_beds_at_provider,
    #     ]
    # )
    df = update_filtering_rule(
        df, rule_name=AscwdsFilteringRule.care_home_location_was_grouped_provider
    )
    return df


def calculate_data_for_grouped_provider_identification(df: DataFrame) -> DataFrame:
    """
    Calculates the variables needed to determine whether a location is likely to be a grouped provider.

    Calculates the variables locations_at_provider, locations_in_ascwds_at_provider, locations_in_ascwds_with_data_at_provider and number_of_beds_at_provider.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with the new variables locations_at_provider, locations_in_ascwds_at_provider, locations_in_ascwds_with_data_at_provider and number_of_beds_at_provider..
    """
    w = Window.partitionBy(
        [IndCQC.provider_id, IndCQC.cqc_location_import_date]
    ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = df.withColumn(
        IndCQC.locations_at_provider, F.count(df[IndCQC.location_id]).over(w)
    )
    df = df.withColumn(
        IndCQC.locations_in_ascwds_at_provider,
        F.count(df[IndCQC.establishment_id]).over(w),
    )
    df = df.withColumn(
        IndCQC.locations_in_ascwds_with_data_at_provider,
        F.count(df[IndCQC.ascwds_filled_posts_dedup_clean]).over(w),
    )
    df = df.withColumn(
        IndCQC.number_of_beds_at_provider, F.sum(df[IndCQC.number_of_beds]).over(w)
    )

    return df


def null_values_which_meet_the_grouped_provider_criteria(df: DataFrame) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple locations, all their ascwds is under one care home location.

    Args:
        df (DataFrame): A dataframe with grouped provider identification columns added.

    Returns:
        DataFrame: A dataframe with grouped providers' care home data nulled.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            (df[IndCQC.care_home] == CareHome.care_home)
            & (
                df[IndCQC.locations_at_provider]
                >= NullGroupedProvidersConfig.max_locations_at_provider
            )
            & (
                df[IndCQC.locations_in_ascwds_at_provider]
                == NullGroupedProvidersConfig.single_location_in_ascwds
            )
            & (
                df[IndCQC.locations_in_ascwds_with_data_at_provider]
                > NullGroupedProvidersConfig.no_locations_with_data
            )
            & (
                df[IndCQC.ascwds_filled_posts_dedup_clean]
                > df[IndCQC.number_of_beds_at_provider]
            ),
            None,
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
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
