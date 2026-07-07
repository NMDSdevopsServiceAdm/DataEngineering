from dataclasses import dataclass, fields

import polars as pl

import polars_utils.cleaning_utils as pUtils
from polars_utils.expressions import is_care_home, is_not_care_home
from polars_utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    NullGroupedProviderColumns as NGPcol,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule


@dataclass
class NullGroupedProvidersConfig:
    """
    Configuration values for defining grouped providers

    Attributes:
        MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY (pl.Float64): Minimum number
            of staff at a care home to allocate as a grouped provider.
        MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY (pl.Float64): Minimum number of
            staff at a non-res location to allocate as a grouped provider.
        POSTS_PER_BED_AT_LOCATION_MULTIPLIER (pl.Int64): Multiplier for the number of
            beds at the individual location.
        POSTS_PER_BED_AT_PROVIDER_MULTIPLIER (pl.Int64): Multiplier for the number of
            beds at the whole provider.
        POSTS_PER_PIR_LOCATION_THRESHOLD (pl.Float64): Threshold for the ratio of
            ASCWDS filled posts to the PIR average for that location.
        POSTS_PER_PIR_PROVIDER_THRESHOLD (pl.Float64): Threshold for the ratio of
            ASCWDS filled posts to the PIR total at the provider.
    """

    MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY: pl.Float64 = 25.0
    MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY: pl.Float64 = 50.0
    POSTS_PER_BED_AT_LOCATION_MULTIPLIER: pl.Int64 = 4
    POSTS_PER_BED_AT_PROVIDER_MULTIPLIER: pl.Int64 = 3
    POSTS_PER_PIR_LOCATION_THRESHOLD: pl.Float64 = 2.5
    POSTS_PER_PIR_PROVIDER_THRESHOLD: pl.Float64 = 1.5


def null_grouped_providers(
    lf: pl.LazyFrame, grouped_providers_lf: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Null ascwds_filled_posts_dedup_clean where a provider has multiple
    locations, all their ascwds is under one location.

    These providers are returned in a separate LazyFrame before being nulled.

    Following analysis of ASCWDS data and contacting some providers, we
    discovered that some providers were submitting their entire workforce
    against one location in ASCWDS, which makes it appear that this location is
    particularly large. We analysed ASCWDS data alongside CQC and Capacity
    Tracker tracker data to investigate instances where a singular location
    within a provider had submitted data to ASCWDS in order to determine how to
    determine which locations looked genuine and which appeared to be the entire
    workforce. Care homes and non residential locations have been analysed
    separately.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame with independent cqc data.
        grouped_providers_lf (pl.LazyFrame): A polars LazyFrame containing existing grouped providers.

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: The input LazyFrame with grouped
            providers' data nulled and a LazyFrame of potential grouped providers prior to nulling.
    """
    lf = calculate_data_for_grouped_provider_identification(lf)

    lf = identify_potential_grouped_providers(lf)

    new_grouped_providers = select_grouped_providers(lf)
    updated_grouped_providers_lf = update_grouped_providers_history(
        new_grouped_providers, grouped_providers_lf
    )

    lf = null_care_home_grouped_providers(lf)
    lf = null_non_residential_grouped_providers(lf)

    ngp_cols = {field.name for field in fields(NGPcol())}
    columns_to_drop = [c for c in lf.collect_schema().names() if c in ngp_cols]

    lf = lf.drop(*columns_to_drop)

    return lf, updated_grouped_providers_lf


def calculate_data_for_grouped_provider_identification(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Calculates the variables needed to determine whether a location is likely to
    be a grouped provider.

    Calculates the variables locations_at_provider,
    locations_in_ascwds_at_provider, locations_in_ascwds_with_data_at_provider
    and number_of_beds_at_provider.

    Args:
        lf (pl.LazyFrame): A LazyFrame with independent cqc data.

    Returns:
        pl.LazyFrame: A LazyFrame with the new variables locations_at_provider,
            locations_in_ascwds_at_provider,
            locations_in_ascwds_with_data_at_provider and
            number_of_beds_at_provider.
    """
    provider_date_group = [IndCQC.provider_id, IndCQC.cqc_location_import_date]
    lf = lf.with_columns(
        pl.mean(IndCQC.pir_people_directly_employed_dedup)
        .over(IndCQC.location_id)
        .alias(NGPcol.location_pir_average)
    )

    summary_cols = {
        NGPcol.count_of_cqc_locations_in_provider: pl.count(IndCQC.location_id),
        NGPcol.count_of_awcwds_locations_in_provider: pl.count(IndCQC.establishment_id),
        NGPcol.count_of_awcwds_locations_with_data_in_provider: pl.count(
            IndCQC.ascwds_filled_posts_dedup_clean
        ),
        NGPcol.number_of_beds_at_provider: pl.sum(IndCQC.number_of_beds),
        NGPcol.provider_pir_count: pl.count(NGPcol.location_pir_average),
        NGPcol.provider_pir_sum: pl.sum(NGPcol.location_pir_average),
    }

    lf = lf.with_columns(
        stats=pl.struct(**summary_cols).over(provider_date_group)
    ).unnest("stats")

    return lf


def identify_potential_grouped_providers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Identify potential grouped providers based on one ASCWDS account for a CQC
    provider with several locations.

    A potential grouped provider is identified on the basis that the provider
    has multiple locations but only one of those locations is in ASCWDS and
    provides filled post data. This function creates a column called
    potential_grouped_provider with True if the location is a potential grouped
    provider and False if not.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame with independent CQC data and
            ASCWDS data.

    Returns:
        pl.LazyFrame: A polars LazyFrame with the new Boolean variable
            potential_grouped_provider.
    """
    lf = lf.with_columns(
        pl.when(
            (pl.col(NGPcol.count_of_cqc_locations_in_provider) > 1)
            & (pl.col(NGPcol.count_of_awcwds_locations_in_provider) == 1)
            & (pl.col(NGPcol.count_of_awcwds_locations_with_data_in_provider) == 1)
        )
        .then(True)
        .otherwise(False)
        .alias(NGPcol.potential_grouped_provider)
    )

    return lf


def null_care_home_grouped_providers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Null ASCWDS data when they have submitted their whole workforce into one
    ASCWDS account.

    By comparing ASCWDS against Capacity Tracker data, there was a large drop in
    accuracy between the values once filled posts were at least triple the
    number of beds in the whole provider, or at least quadruple the number of
    beds in that particular location.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame with independent CQC data and ASCWDS data.

    Returns:
        pl.LazyFrame: A polars LazyFrame with grouped providers' care home data nulled.
    """
    location_identified_as_a_potential_grouped_provider = (
        pl.col(NGPcol.potential_grouped_provider) == True
    )
    ascwds_filled_posts_above_minimum_size_to_identify = (
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
        >= NullGroupedProvidersConfig.MINIMUM_SIZE_OF_CARE_HOME_LOCATION_TO_IDENTIFY
    )
    ascwds_filled_posts_above_location_threshold = pl.col(
        IndCQC.ascwds_filled_posts_dedup_clean
    ) >= NullGroupedProvidersConfig.POSTS_PER_BED_AT_LOCATION_MULTIPLIER * pl.col(
        IndCQC.number_of_beds
    )

    ascwds_filled_posts_above_provider_threshold = pl.col(
        IndCQC.ascwds_filled_posts_dedup_clean
    ) >= NullGroupedProvidersConfig.POSTS_PER_BED_AT_PROVIDER_MULTIPLIER * pl.col(
        NGPcol.number_of_beds_at_provider
    )

    lf = lf.with_columns(
        pl.when(
            is_care_home()
            & location_identified_as_a_potential_grouped_provider
            & ascwds_filled_posts_above_minimum_size_to_identify
            & (
                ascwds_filled_posts_above_location_threshold
                | ascwds_filled_posts_above_provider_threshold
            )
        )
        .then(pl.lit(None))
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .alias(IndCQC.ascwds_filled_posts_dedup_clean)
    )

    lf = pUtils.calculate_filled_posts_per_bed_ratio(
        lf, IndCQC.ascwds_filled_posts_dedup_clean, IndCQC.filled_posts_per_bed_ratio
    )

    lf = update_filtering_rule(
        lf,
        IndCQC.ascwds_filtering_rule,
        IndCQC.ascwds_filled_posts_dedup,
        IndCQC.ascwds_filled_posts_dedup_clean,
        AscwdsFilteringRule.populated,
        AscwdsFilteringRule.care_home_location_was_grouped_provider,
    )
    return lf


def null_non_residential_grouped_providers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Null ASCWDS data when they have submitted their whole workforce into one
    ASCWDS account.

    We have discovered that some locations join ASCWDS and submit their entire
    workforce in one location, which makes it appear that this location is
    particularly large.

    If the location looks like it is a grouped provider (based on the CQC
    provider having multiple locations, but only one of those locations is in
    ASCWDS), we will remove the ASCWDS data for that location if the filled
    posts are significantly larger than the average PIR for that location or the
    total PIR for all locations in that provider.

    Args:
        lf (pl.LazyFrame): A polars LazyFrame with independent CQC data and ASCWDS
            data.

    Returns:
        pl.LazyFrame: A polars LazyFrame with grouped providers' non-residential
            data nulled.
    """
    location_identified_as_a_potential_grouped_provider = (
        pl.col(NGPcol.potential_grouped_provider) == True
    )
    ascwds_filled_posts_above_minimum_size_to_identify = (
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
        >= NullGroupedProvidersConfig.MINIMUM_SIZE_OF_NON_RES_LOCATION_TO_IDENTIFY
    )
    location_has_submitted_pir_data = pl.col(NGPcol.location_pir_average).is_not_null()
    ascwds_exceeds_pir_location_threshold = (
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean)
        / pl.col(NGPcol.location_pir_average)
    ) >= NullGroupedProvidersConfig.POSTS_PER_PIR_LOCATION_THRESHOLD
    ascwds_exceeds_pir_provider_threshold = (
        pl.col(IndCQC.ascwds_filled_posts_dedup_clean) / pl.col(NGPcol.provider_pir_sum)
    ) >= NullGroupedProvidersConfig.POSTS_PER_PIR_PROVIDER_THRESHOLD
    multiple_locations_submitted_pir_data_at_provider = (
        pl.col(NGPcol.provider_pir_count) > 1
    )

    lf = lf.with_columns(
        pl.when(
            is_not_care_home()
            & location_identified_as_a_potential_grouped_provider
            & ascwds_filled_posts_above_minimum_size_to_identify
            & location_has_submitted_pir_data
            & (
                ascwds_exceeds_pir_location_threshold
                | (
                    ascwds_exceeds_pir_provider_threshold
                    & multiple_locations_submitted_pir_data_at_provider
                )
            )
        )
        .then(pl.lit(None))
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .alias(IndCQC.ascwds_filled_posts_dedup_clean)
    )

    lf = update_filtering_rule(
        lf,
        IndCQC.ascwds_filtering_rule,
        IndCQC.ascwds_filled_posts_dedup,
        IndCQC.ascwds_filled_posts_dedup_clean,
        AscwdsFilteringRule.populated,
        AscwdsFilteringRule.non_res_location_was_grouped_provider,
    )

    return lf


def select_grouped_providers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters the input LazyFrame to the following:
        - potential_grouped_provider is True
        - cqc_location_import_date equal to max year/month across dataset.

    Args:
        lf (pl.LazyFrame): A LazyFrame with potential_grouped_provider column.

    Returns:
        pl.LazyFrame: The filtered input LazyFrame with `grouped_provider_status`
            and `last_update_date` columns added.
    """
    cols_to_select = [
        IndCQC.location_id,
        IndCQC.provider_id,
        IndCQC.cqc_location_import_date,
        AWPClean.nmds_id,
        NGPcol.potential_grouped_provider,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.care_home,
        IndCQC.number_of_beds,
    ]

    date_col = pl.col(IndCQC.cqc_location_import_date)
    trunc_date_col = date_col.dt.truncate("1mo")  # E.g. 2026-01-05 becomes 2026-01-01.

    return (
        lf.filter(pl.col(NGPcol.potential_grouped_provider))
        .filter(trunc_date_col == trunc_date_col.max())
        .select(cols_to_select)
        .with_columns(
            pl.lit("problem").alias(NGPcol.grouped_provider_status),
            pl.col(IndCQC.cqc_location_import_date).alias(
                NGPcol.grp_prov_identified_date
            ),
            pl.lit(None).cast(pl.Date).alias(NGPcol.grp_prov_fixed_date),
        )
    )


def update_grouped_providers_history(
    new_grouped_providers_lf: pl.LazyFrame,
    grouped_providers_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Merges newly identified grouped providers with the historical records,
    updating status for locations that have dropped off.

    On first run (empty grouped_providers_lf), returns only the new snapshot.
    On subsequent runs:
        - Locations no longer in the new snapshot have grouped_provider_status set to
          "fixed" and grp_prov_fixed_date set to the snapshot's import date.
        - Locations still active are retained as-is with last_update_date unchanged.
        - Duplicates on (location_id, grouped_provider_status) are dropped,
          keeping the oldest import date.

    Args:
        new_grouped_providers_lf (pl.LazyFrame): Current snapshot of grouped
            providers, as returned by select_grouped_providers function.
        grouped_providers_lf (pl.LazyFrame): Historical grouped provider records.
            May be empty on first run.

    Returns:
        pl.LazyFrame: Full history of grouped provider records with updated statuses.
            Unique on (location_id, grouped_provider_status).
    """
    if grouped_providers_lf.limit(1).collect().is_empty():
        return new_grouped_providers_lf

    new_grouped_provider_ids = new_grouped_providers_lf.select(IndCQC.location_id)

    snapshot_date = (
        new_grouped_providers_lf.select(pl.col(IndCQC.cqc_location_import_date).max())
        .collect()
        .item()
    )

    fixed_grouped_providers_lf = grouped_providers_lf.join(
        new_grouped_provider_ids, on=IndCQC.location_id, how="anti"
    ).with_columns(
        pl.lit("fixed").alias(NGPcol.grouped_provider_status),
        pl.lit(snapshot_date).alias(NGPcol.grp_prov_fixed_date),
    )

    active_grouped_providers_lf = grouped_providers_lf.join(
        new_grouped_provider_ids, on=IndCQC.location_id, how="semi"
    )

    return (
        pl.concat(
            [
                active_grouped_providers_lf,
                fixed_grouped_providers_lf,
                new_grouped_providers_lf,
            ]
        )
        .sort(IndCQC.cqc_location_import_date, descending=False)
        .unique(
            subset=[IndCQC.location_id, NGPcol.grouped_provider_status],
            keep="first",
            maintain_order=True,
        )
    )
