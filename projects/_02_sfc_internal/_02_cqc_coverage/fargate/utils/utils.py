import polars as pl

from polars_utils.cleaning_utils import add_aligned_date_column
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
    CQCLatestRating,
    InAscwds,
)


def _keep_first_row_per_group(
    lf: pl.LazyFrame,
    group_columns: list[str],
    order_columns: list[str],
    descending: list[bool],
) -> pl.LazyFrame:
    """Keeps one row per `group_columns` group, the first by `order_columns`.

    Args:
        lf (pl.LazyFrame): The LazyFrame to deduplicate.
        group_columns (list[str]): Columns identifying duplicate groups.
        order_columns (list[str]): Columns to sort by (highest priority
            first) to decide which row of each group is kept.
        descending (list[bool]): Sort direction for each of `order_columns`.

    Returns:
        pl.LazyFrame: One row per `group_columns` group.
    """
    return lf.sort(by=order_columns, descending=descending).unique(
        subset=group_columns, keep="first", maintain_order=True
    )


def add_removed_by_purge_date_filter_flag(
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Flags ASC-WDS workplaces that have passed their purge date.

    Args:
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: The same data with a `removed_by_purge_date_filter` flag added.
    """
    return ascwds_workplace_lf.with_columns(
        (
            pl.col(AWPClean.workplace_last_active_date) < pl.col(AWPClean.purge_date)
        ).alias(AWPClean.removed_by_purge_date_filter)
    ).drop(AWPClean.workplace_last_active_date, AWPClean.purge_date)


def deduplicate_ascwds_workplace_data(
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Deduplicates ASC-WDS workplace data to one row per import date and location.

    Args:
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: One row per import date and location.
    """
    return _keep_first_row_per_group(
        ascwds_workplace_lf,
        group_columns=[AWPClean.ascwds_workplace_import_date, AWPClean.location_id],
        order_columns=[AWPClean.master_update_date, AWPClean.establishment_id],
        descending=[True, False],
    )


def join_ascwds_data_into_cqc_location_df(
    cqc_location_lf: pl.LazyFrame,
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Joins ASC-WDS workplace data onto CQC locations using an aligned import date.

    Args:
        cqc_location_lf (pl.LazyFrame): Cleaned CQC locations data.
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: CQC locations with ASC-WDS workplace columns joined in.
    """
    cqc_location_lf = add_aligned_date_column(
        cqc_location_lf,
        ascwds_workplace_lf,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    ascwds_workplace_lf = ascwds_workplace_lf.rename(
        {AWPClean.location_id: CQCLClean.location_id}
    )

    return cqc_location_lf.join(
        ascwds_workplace_lf,
        on=[CQCLClean.location_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )


def add_flag_for_in_ascwds(merged_coverage_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds a flag for whether each CQC location is present and active in ASC-WDS.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data with ASC-WDS columns joined
            in.

    Returns:
        pl.LazyFrame: The same data with an `in_ascwds` column added.
    """
    return merged_coverage_lf.with_columns(
        pl.when(
            pl.col(AWPClean.establishment_id).is_not_null()
            & (pl.col(AWPClean.removed_by_purge_date_filter) == False)  # noqa: E712
        )
        .then(pl.lit(InAscwds.is_in_ascwds))
        .otherwise(pl.lit(InAscwds.not_in_ascwds))
        .alias(CoverageColumns.in_ascwds)
    )


def deduplicate_merged_coverage_data(merged_coverage_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Deduplicates the merged coverage data to one row per location and import date.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data with ASC-WDS columns joined
            in and the `in_ascwds` flag added.

    Returns:
        pl.LazyFrame: One row per import date, name, postcode and care home.
    """
    return _keep_first_row_per_group(
        merged_coverage_lf,
        group_columns=[
            CQCLClean.cqc_location_import_date,
            CQCLClean.name,
            CQCLClean.postal_code,
            CQCLClean.care_home,
        ],
        order_columns=[
            CoverageColumns.in_ascwds,
            CQCLClean.imputed_registration_date,
            CQCLClean.location_id,
        ],
        descending=[True, False, False],
    )


def _filter_for_latest_cqc_ratings(cqc_ratings_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filters CQC ratings down to the latest current rating per location.

    Args:
        cqc_ratings_lf (pl.LazyFrame): CQC ratings data.

    Returns:
        pl.LazyFrame: CQC ratings data with only the latest current rating per
            location.
    """
    return cqc_ratings_lf.unique().filter(
        (
            pl.col(CQCRatingsColumns.latest_rating_flag)
            == CQCLatestRating.is_latest_rating
        )
        & (
            pl.col(CQCRatingsColumns.current_or_historic)
            == CQCCurrentOrHistoricValues.current
        )
    )


def join_latest_cqc_rating_into_coverage_df(
    merged_coverage_lf: pl.LazyFrame,
    cqc_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Joins each location's latest current CQC rating onto the coverage data.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data so far.
        cqc_ratings_lf (pl.LazyFrame): CQC ratings data.

    Returns:
        pl.LazyFrame: Coverage data with the latest overall CQC rating added.
    """
    latest_cqc_ratings_lf = _filter_for_latest_cqc_ratings(cqc_ratings_lf)

    return merged_coverage_lf.join(
        latest_cqc_ratings_lf,
        on=CQCLClean.location_id,
        how="left",
    ).drop(CQCRatingsColumns.latest_rating_flag, CQCRatingsColumns.current_or_historic)


def add_columns_for_locality_manager_dashboard(
    merged_coverage_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Adds the locality manager dashboard columns to the coverage data.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data so far.

    Returns:
        pl.LazyFrame: Coverage data with the locality manager dashboard columns
            added.
    """
    # TODO (ticket 2131c): migrate the 6 functions currently in
    # `lm_engagement_utils.py` (LA coverage, coverage monthly change, locations
    # monthly change, new registrations) into this module and call them here.
    return merged_coverage_lf


def join_provider_name_into_merged_coverage_df(
    merged_coverage_lf: pl.LazyFrame,
    cqc_providers_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Joins each location's latest provider name onto the coverage data.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data so far.
        cqc_providers_lf (pl.LazyFrame): CQC providers data.

    Returns:
        pl.LazyFrame: Coverage data with the provider name added.
    """
    deduplicated_providers_lf = _keep_first_row_per_group(
        cqc_providers_lf,
        group_columns=[CQCPClean.provider_id],
        order_columns=[CQCPClean.cqc_provider_import_date],
        descending=[True],
    ).select(
        CQCPClean.provider_id,
        pl.col(CQCPClean.name).alias(CQCLClean.provider_name),
    )

    original_column_order = merged_coverage_lf.collect_schema().names()

    return merged_coverage_lf.join(
        deduplicated_providers_lf,
        on=CQCPClean.provider_id,
        how="left",
    ).select(*original_column_order, CQCLClean.provider_name)
