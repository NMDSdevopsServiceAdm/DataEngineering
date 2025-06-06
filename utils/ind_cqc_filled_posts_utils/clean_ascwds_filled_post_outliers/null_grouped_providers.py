from dataclasses import dataclass, fields

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
from projects.utils.utils.utils import calculate_windowed_column


@dataclass
class NullGroupedProvidersConfig:
    """Configuration values for defining grouped providers

    Attributes:
        POSTS_PER_BED_AT_PROVIDER_MULTIPLIER (int): Multiplier for the number of beds at the whole provider.
        POSTS_PER_BED_AT_LOCATION_MULTIPLIER (int): Multiplier for the number of beds at the individual location.
        MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY (float): Minimum number of staff at a care home to allocate as a grouped provider.
        MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY (float): Minimum number of staff at a non-res location to allocate as a grouped provider.
        PIR_LOCATION_MULTIPLIER (float): Multiplier for the ratio of ASCWDS filled posts to the PIR average for that location.
        PIR_PROVIDER_MULTIPLIER (float): Multiplier for the ratio of ASCWDS filled posts to the PIR total at the provider.
    """

    POSTS_PER_BED_AT_PROVIDER_MULTIPLIER: int = 3
    POSTS_PER_BED_AT_LOCATION_MULTIPLIER: int = 4
    MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY: float = 25.0
    MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY: float = 50.0
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

    columns_to_drop = [field.name for field in fields(NGPcol())]
    df = df.drop(*columns_to_drop)

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
    prov_w = Window.partitionBy([IndCQC.provider_id, IndCQC.cqc_location_import_date])

    df = calculate_windowed_column(
        df,
        loc_w,
        NGPcol.location_pir_average,
        IndCQC.pir_people_directly_employed_dedup,
        "avg",
    )
    df = calculate_windowed_column(
        df,
        prov_w,
        NGPcol.count_of_cqc_locations_in_provider,
        IndCQC.location_id,
        "count",
    )
    df = calculate_windowed_column(
        df,
        prov_w,
        NGPcol.count_of_awcwds_locations_in_provider,
        IndCQC.establishment_id,
        "count",
    )
    df = calculate_windowed_column(
        df,
        prov_w,
        NGPcol.count_of_awcwds_locations_with_data_in_provider,
        IndCQC.ascwds_filled_posts_dedup_clean,
        "count",
    )
    df = calculate_windowed_column(
        df, prov_w, NGPcol.number_of_beds_at_provider, IndCQC.number_of_beds, "sum"
    )
    df = calculate_windowed_column(
        df, prov_w, NGPcol.provider_pir_count, NGPcol.location_pir_average, "count"
    )
    df = calculate_windowed_column(
        df, prov_w, NGPcol.provider_pir_sum, NGPcol.location_pir_average, "sum"
    )

    return df


def identify_potential_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Identifies whether a location is potentially a grouped provider.

    A potential grouped provider is identified on the basis that the provider has multiple
    locations but only one of those locations is in ASCWDS and provides filled post data.
    This function creates a column called potential_grouped_provider with True if the
    location is a potential grouped provider and False if not.

    Args:
        df (DataFrame): A dataframe with independent cqc data.

    Returns:
        DataFrame: A dataframe with the new Boolean variable potential_grouped_provider.
    """
    df = df.withColumn(
        NGPcol.potential_grouped_provider,
        F.when(
            (df[NGPcol.count_of_cqc_locations_in_provider] > 1)
            & (df[NGPcol.count_of_awcwds_locations_in_provider] == 1)
            & (df[NGPcol.count_of_awcwds_locations_with_data_in_provider] == 1),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    return df


def null_care_home_grouped_providers(df: DataFrame) -> DataFrame:
    """
    Null ASCWDS data when they have submitted their whole workforce into one ASCWDS account.

    By comparing ASCWDS against Capacity Tracker data, there was a large drop in accuracy between the values once filled
    posts were at least triple the number of beds in the whole provider, or at least quadruple the number of beds in that
    particular location.

    Args:
        df (DataFrame): A DataFrame with independent CQC data and ASCWDS data.

    Returns:
        DataFrame: A dataframe with grouped providers' care home data nulled.
    """
    location_is_a_care_home = df[IndCQC.care_home] == CareHome.care_home
    location_identified_as_a_potential_grouped_provider = (
        df[NGPcol.potential_grouped_provider] == True
    )
    ascwds_filled_posts_above_minimum_size_to_identify = (
        df[IndCQC.ascwds_filled_posts_dedup_clean]
        >= NullGroupedProvidersConfig.MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY
    )
    ascwds_filled_posts_above_location_threshold = (
        df[IndCQC.ascwds_filled_posts_dedup_clean]
        >= NullGroupedProvidersConfig.POSTS_PER_BED_AT_LOCATION_MULTIPLIER
        * df[IndCQC.number_of_beds]
    )
    ascwds_filled_posts_above_provider_threshold = (
        df[IndCQC.ascwds_filled_posts_dedup_clean]
        >= NullGroupedProvidersConfig.POSTS_PER_BED_AT_PROVIDER_MULTIPLIER
        * df[NGPcol.number_of_beds_at_provider]
    )

    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            location_is_a_care_home
            & location_identified_as_a_potential_grouped_provider
            & ascwds_filled_posts_above_minimum_size_to_identify
            & (
                ascwds_filled_posts_above_location_threshold
                | ascwds_filled_posts_above_provider_threshold
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
        df (DataFrame): A DataFrame with independent CQC data and ASCWDS data.

    Returns:
        DataFrame: A dataframe with grouped providers' non-residential data nulled.
    """
    location_is_not_a_care_home = df[IndCQC.care_home] == CareHome.not_care_home
    location_identified_as_a_potential_grouped_provider = (
        df[NGPcol.potential_grouped_provider] == True
    )
    ascwds_filled_posts_above_minimum_size_to_identify = (
        df[IndCQC.ascwds_filled_posts_dedup_clean]
        >= NullGroupedProvidersConfig.MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY
    )
    location_has_submitted_pir_data = df[NGPcol.location_pir_average].isNotNull()
    ascwds_exceeds_pir_location_threshold = (
        df[IndCQC.ascwds_filled_posts_dedup_clean] / df[NGPcol.location_pir_average]
    ) >= NullGroupedProvidersConfig.PIR_LOCATION_MULTIPLIER
    ascwds_exceeds_pir_provider_threshold = (
        df[IndCQC.ascwds_filled_posts_dedup_clean] / df[NGPcol.provider_pir_sum]
    ) >= NullGroupedProvidersConfig.PIR_PROVIDER_MULTIPLIER
    multiple_locations_submitted_pir_data_at_provider = (
        df[NGPcol.provider_pir_count] > 1
    )

    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            location_is_not_a_care_home
            & location_identified_as_a_potential_grouped_provider
            & ascwds_filled_posts_above_minimum_size_to_identify
            & location_has_submitted_pir_data
            & (
                ascwds_exceeds_pir_location_threshold
                | (
                    ascwds_exceeds_pir_provider_threshold
                    & multiple_locations_submitted_pir_data_at_provider
                )
            ),
            None,
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )

    df = update_filtering_rule(
        df, rule_name=AscwdsFilteringRule.non_res_location_was_grouped_provider
    )

    return df
