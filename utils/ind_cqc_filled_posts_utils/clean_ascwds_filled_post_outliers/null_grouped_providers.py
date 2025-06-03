from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, Window

import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule, CareHome
from utils.ind_cqc_filled_posts_utils.clean_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


@dataclass
class NullGroupedProvidersConfig:
    """Configuration values for defining grouped providers

    Attributes:
        MULTIPLE_LOCATIONS_AT_PROVIDER_IDENTIFIER (int): Identifier for providers with multiple locations.
        SINGLE_LOCATION_IDENTIFIER (int): Identifier for a single location.
        NUMBER_OF_BEDS_AT_PROVIDER_MULTIPLIER (int): Multiplier for the number of beds at the whole provider.
        NUMBER_OF_BEDS_AT_LOCATION_MULTIPLIER (int): Multiplier for the number of beds at the individual location.
        MINIMUM_SIZE_OF_LOCATION_TO_IDENTIFY (int): Minimum number of staff to allocate as a grouped provider.
    """

    MULTIPLE_LOCATIONS_AT_PROVIDER_IDENTIFIER: int = 2
    SINGLE_LOCATION_IDENTIFIER: int = 1
    NUMBER_OF_BEDS_AT_PROVIDER_MULTIPLIER: int = 3
    NUMBER_OF_BEDS_AT_LOCATION_MULTIPLIER: int = 4
    MINIMUM_SIZE_OF_LOCATION_TO_IDENTIFY: int = 50


def null_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple locations, all their ascwds is under one location.

    Following analysis of ASCWDS data and contacting some providers, we discovered that some providers were submitting
    their entire workforce against one location in ASCWDS, which makes it appear that this location is particularly large.
    We analysed ASCWDS data alongside CQC and Capacity Tracker tracker data to investigate instances where a singular
    location within a provider had submitted data to ASCWDS in order to determine how to determine which locations looked
    genuine and which appeared to be the entire workforce. Care homes and non residential locations have been analysed
    separately.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with grouped providers' data nulled.
    """
    df = calculate_data_for_grouped_provider_identification(df)
    df = identify_potential_grouped_providers(df)

    df = null_care_home_grouped_providers(df)
    df = null_non_residential_grouped_providers(df)
    df = df.drop(
        *[
            IndCQC.locations_at_provider_count,
            IndCQC.locations_in_ascwds_at_provider_count,
            IndCQC.locations_in_ascwds_with_data_at_provider_count,
            IndCQC.number_of_beds_at_provider,
            # IndCQC.potential_grouped_provider,
        ]
    )
    return df


def calculate_data_for_grouped_provider_identification(df: DataFrame) -> DataFrame:
    """
    Calculates the variables needed to determine whether a location is likely to be a grouped provider.

    Calculates the variables locations_at_provider, locations_in_ascwds_at_provider, locations_in_ascwds_with_data_at_provider and number_of_beds_at_provider.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with the new variables locations_at_provider, locations_in_ascwds_at_provider, locations_in_ascwds_with_data_at_provider and number_of_beds_at_provider.
    """
    w = Window.partitionBy(
        [IndCQC.provider_id, IndCQC.cqc_location_import_date]
    ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df = df.withColumn(
        IndCQC.locations_at_provider_count, F.count(df[IndCQC.location_id]).over(w)
    )
    df = df.withColumn(
        IndCQC.locations_in_ascwds_at_provider_count,
        F.count(df[IndCQC.establishment_id]).over(w),
    )
    df = df.withColumn(
        IndCQC.locations_in_ascwds_with_data_at_provider_count,
        F.count(df[IndCQC.ascwds_filled_posts_dedup_clean]).over(w),
    )
    df = df.withColumn(
        IndCQC.number_of_beds_at_provider, F.sum(df[IndCQC.number_of_beds]).over(w)
    )

    return df


def identify_potential_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Identifies whether a location is potentially a grouped provider.

    A potential grouped provider is identified on the basis that the provider has multiple locations but only one of those locations is in ASCWDS and provides filled post data. This function creates a column called potential_grouped_provider with True if the location is a potential grouped provider and False if not.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with the new Boolean variable potential_grouped_provider.
    """
    df = df.withColumn(
        IndCQC.potential_grouped_provider,
        F.when(
            (
                df[IndCQC.locations_at_provider_count]
                >= NullGroupedProvidersConfig.MULTIPLE_LOCATIONS_AT_PROVIDER_IDENTIFIER
            )
            & (
                df[IndCQC.locations_in_ascwds_at_provider_count]
                == NullGroupedProvidersConfig.SINGLE_LOCATION_IDENTIFIER
            )
            & (
                df[IndCQC.locations_in_ascwds_with_data_at_provider_count]
                == NullGroupedProvidersConfig.SINGLE_LOCATION_IDENTIFIER
            ),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    return df


def null_care_home_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple locations, all their ascwds is under one care home location.

    By comparing ASCWDS against Capacity Tracker data, there was a large drop in accuracy between the values once filled
    posts were at least triple the number of beds in the whole provider, or at least quadruple the number of beds in that
    particular location.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with grouped providers' care home data nulled.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            (df[IndCQC.care_home] == CareHome.care_home)
            & (df[IndCQC.potential_grouped_provider] == True)
            & (
                df[IndCQC.ascwds_filled_posts_dedup_clean]
                >= NullGroupedProvidersConfig.MINIMUM_SIZE_OF_LOCATION_TO_IDENTIFY
            )
            & (
                (
                    df[IndCQC.ascwds_filled_posts_dedup_clean]
                    >= NullGroupedProvidersConfig.NUMBER_OF_BEDS_AT_LOCATION_MULTIPLIER
                    * df[IndCQC.number_of_beds]
                )
                | (
                    df[IndCQC.ascwds_filled_posts_dedup_clean]
                    >= NullGroupedProvidersConfig.NUMBER_OF_BEDS_AT_PROVIDER_MULTIPLIER
                    * df[IndCQC.number_of_beds_at_provider]
                )
            ),
            None,
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )

    df = cUtils.calculate_filled_posts_per_bed_ratio(
        df, IndCQC.ascwds_filled_posts_dedup_clean
    )

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
    loc_w = Window.partitionBy(IndCQC.location_id)

    df = df.withColumn(
        "avg_pir_at_location",
        F.avg(df[IndCQC.pir_people_directly_employed_dedup]).over(loc_w),
    )

    # df = df.withColumn(
    #     IndCQC.ascwds_filled_posts_dedup_clean,
    #     F.when(
    #         (df[IndCQC.care_home] == CareHome.not_care_home)
    #         & (df[IndCQC.potential_grouped_provider] == True)
    #         & (
    #             (
    #                 df[IndCQC.ascwds_filled_posts_dedup_clean] # TODO
    #             )
    #             | (
    #                 df[IndCQC.ascwds_filled_posts_dedup_clean] # TODO
    #             )
    #         ),
    #         None,
    #     ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    # )

    # df = update_filtering_rule(
    #     df, rule_name=AscwdsFilteringRule.non_res_location_was_grouped_provider
    # )
    return df
