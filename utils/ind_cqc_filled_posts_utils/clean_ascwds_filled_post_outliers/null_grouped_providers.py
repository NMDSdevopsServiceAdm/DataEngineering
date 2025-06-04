from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, Window

import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    NullGroupedProviderColumns as NGPcol,
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
        PIR_LOCATION_MULTIPLIER (float): Multiplier for the ratio of ASCWDS filled posts to the PIR average for that location.
        PIR_PROVIDER_MULTIPLIER (float): Multiplier for the ratio of ASCWDS filled posts to the PIR total at the provider.
    """

    MULTIPLE_LOCATIONS_AT_PROVIDER_IDENTIFIER: int = 2
    SINGLE_LOCATION_IDENTIFIER: int = 1
    NUMBER_OF_BEDS_AT_PROVIDER_MULTIPLIER: int = 3
    NUMBER_OF_BEDS_AT_LOCATION_MULTIPLIER: int = 4
    MINIMUM_SIZE_OF_LOCATION_TO_IDENTIFY: int = 50
    PIR_LOCATION_MULTIPLIER: float = 2.5
    PIR_PROVIDER_MULTIPLIER: float = 1.5


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
            NGPcol.count_of_cqc_locations_in_provider,
            NGPcol.count_of_awcwds_locations_in_provider,
            NGPcol.count_of_awcwds_locations_with_data_in_provider,
            NGPcol.number_of_beds_at_provider,
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
    loc_w = Window.partitionBy(IndCQC.location_id)
    df = df.withColumn(
        NGPcol.location_pir_average,
        F.avg(df[IndCQC.pir_people_directly_employed_dedup]).over(loc_w),
    )

    prov_w = Window.partitionBy([IndCQC.provider_id, IndCQC.cqc_location_import_date])
    df = df.withColumn(
        NGPcol.count_of_cqc_locations_in_provider,
        F.count(df[IndCQC.location_id]).over(prov_w),
    )
    df = df.withColumn(
        NGPcol.count_of_awcwds_locations_in_provider,
        F.count(df[IndCQC.establishment_id]).over(prov_w),
    )
    df = df.withColumn(
        NGPcol.count_of_awcwds_locations_with_data_in_provider,
        F.count(df[IndCQC.ascwds_filled_posts_dedup_clean]).over(prov_w),
    )
    df = df.withColumn(
        NGPcol.number_of_beds_at_provider, F.sum(df[IndCQC.number_of_beds]).over(prov_w)
    )
    df = df.withColumn(
        NGPcol.provider_pir_count, F.count(df[NGPcol.location_pir_average]).over(prov_w)
    )
    df = df.withColumn(
        NGPcol.provider_pir_sum, F.sum(df[NGPcol.location_pir_average]).over(prov_w)
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
                df[NGPcol.count_of_cqc_locations_in_provider]
                >= NullGroupedProvidersConfig.MULTIPLE_LOCATIONS_AT_PROVIDER_IDENTIFIER
            )
            & (
                df[NGPcol.count_of_awcwds_locations_in_provider]
                == NullGroupedProvidersConfig.SINGLE_LOCATION_IDENTIFIER
            )
            & (
                df[NGPcol.count_of_awcwds_locations_with_data_in_provider]
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
                    * df[NGPcol.number_of_beds_at_provider]
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
    Null ASCWDS data when they have submitted their whole workforce into one ASCWDS account.

    We have discovered that some locations join ASCWDS and submit their entire workforce in one
    location, which makes it appear that this location is particularly large.

    If the location looks like it is a grouped provider (based on the CQC provider having multiple
    locations, but only one of those locations is in ASCWDS), we will remove the ASCWDS data
    for that location if the filled posts are significantly larger than the average PIR for that
    location or the total PIR for all locations in that provider.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with grouped providers' non-residential data nulled.
    """
    ascwds_to_pir_location_ratio = (
        df[IndCQC.ascwds_filled_posts_dedup_clean] / df[NGPcol.location_pir_average]
    )
    ascwds_exceeds_pir_location_threshold = (
        ascwds_to_pir_location_ratio
        >= NullGroupedProvidersConfig.PIR_LOCATION_MULTIPLIER
    )

    ascwds_to_pir_provider_ratio = (
        df[IndCQC.ascwds_filled_posts_dedup_clean] / df[NGPcol.provider_pir_sum]
    )
    ascwds_exceeds_pir_provider_threshold = (
        ascwds_to_pir_provider_ratio
        >= NullGroupedProvidersConfig.PIR_PROVIDER_MULTIPLIER
    ) & (
        df[NGPcol.provider_pir_count]
        > NullGroupedProvidersConfig.SINGLE_LOCATION_IDENTIFIER
    )

    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            (df[IndCQC.care_home] == CareHome.not_care_home)
            & (df[IndCQC.potential_grouped_provider] == True)
            & (df[NGPcol.location_pir_average].isNotNull())
            & (
                ascwds_exceeds_pir_location_threshold
                | (ascwds_exceeds_pir_provider_threshold)
            ),
            None,
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )

    df = update_filtering_rule(
        df, rule_name=AscwdsFilteringRule.non_res_location_was_grouped_provider
    )
    return df
