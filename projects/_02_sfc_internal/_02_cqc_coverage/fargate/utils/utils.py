import polars as pl


def add_removed_by_purge_date_filter_flag(
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Flags ASC-WDS workplaces that should be excluded for having passed their purge date.

    Args:
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data, including its
            last-active and purge-date columns.

    Returns:
        pl.LazyFrame: The same data with a `removed_by_purge_date_filter` flag added
            and its source last-active/purge-date columns dropped.
    """
    # TODO: add `removed_by_purge_date_filter` = last_active_date < purge_date, then
    # drop the last-active-date and purge-date columns.
    return ascwds_workplace_lf


def deduplicate_ascwds_workplace_data(
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Deduplicates ASC-WDS workplace data to one row per import date and location.

    Args:
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: The same data with one row per (import date, location),
            preferring the most recently updated establishment.
    """
    # TODO: dedupe on (ascwds_workplace_import_date, location_id), preferring the row
    # with the latest master_update_date, then the lowest establishment_id.
    return ascwds_workplace_lf


def join_ascwds_data_into_cqc_location_df(
    cqc_location_lf: pl.LazyFrame,
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Joins ASC-WDS workplace data onto CQC locations using an aligned import date.

    Aligns each CQC location's import date to the most recent ASC-WDS import date
    on or before it, then joins on location_id and that aligned date, bringing in
    all ASC-WDS workplace columns.

    Args:
        cqc_location_lf (pl.LazyFrame): Cleaned CQC locations data.
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: CQC locations with ASC-WDS workplace columns joined in.
    """
    # TODO: add an aligned ASC-WDS import date column onto cqc_location_lf (equal to
    # or before each location's own CQC import date).
    # TODO: rename ascwds_workplace_lf's location_id column to match the CQC
    # locations location_id column name, then left-join on [location_id, aligned
    # import date].
    return cqc_location_lf


def add_flag_for_in_ascwds(merged_coverage_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds a flag for whether each CQC location is present and active in ASC-WDS.

    A location is flagged as in ASC-WDS when it has an ASC-WDS establishment_id and
    has not been excluded by the purge-date filter.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data with ASC-WDS columns joined
            in.

    Returns:
        pl.LazyFrame: The same data with an `in_ascwds` column added.
    """
    # TODO: add in_ascwds = 1 where establishment_id is not null and
    # removed_by_purge_date_filter is False, else 0.
    return merged_coverage_lf


def deduplicate_merged_coverage_data(merged_coverage_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Deduplicates the merged coverage data to one row per location and import date.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data with ASC-WDS columns joined
            in and the `in_ascwds` flag added.

    Returns:
        pl.LazyFrame: The same data with one row per (import date, name, postcode,
            care home), preferring the row that is in ASC-WDS.
    """
    # TODO: dedupe on (cqc_location_import_date, name, postal_code, care_home),
    # preferring in_ascwds descending, then imputed_registration_date ascending, then
    # location_id ascending.
    return merged_coverage_lf


def join_latest_cqc_rating_into_coverage_df(
    merged_coverage_lf: pl.LazyFrame,
    cqc_ratings_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Joins each location's latest current CQC rating onto the coverage data.

    Internally filters cqc_ratings_lf down to the latest current rating per location
    before joining, since the raw ratings data contains multiple rows per location.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data so far.
        cqc_ratings_lf (pl.LazyFrame): CQC ratings data (may contain multiple rows
            per location).

    Returns:
        pl.LazyFrame: Coverage data with the latest overall CQC rating added.
    """
    # TODO: filter cqc_ratings_lf to latest_rating_flag == 1 and current_or_historic
    # == "current" (ticket 2131c — this will likely become its own
    # filter_for_latest_cqc_ratings helper, mirroring the PySpark original).
    # TODO: left-join the filtered ratings onto merged_coverage_lf on location_id.
    return merged_coverage_lf


def add_columns_for_locality_manager_dashboard(
    merged_coverage_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Adds the locality manager dashboard columns to the coverage data.

    Covers local-authority coverage, month-on-month coverage/location change, and
    new-registration counts. Migrated in full as its own ticket (2131c).

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data so far.

    Returns:
        pl.LazyFrame: Coverage data with the locality manager dashboard columns
            added.
    """
    # TODO (ticket 2131c): migrate the 6 functions currently in the PySpark
    # `lm_engagement_utils.py` (LA coverage, coverage monthly change, locations
    # monthly change, new registrations) into this module and call them here.
    return merged_coverage_lf


def join_provider_name_into_merged_coverage_df(
    merged_coverage_lf: pl.LazyFrame,
    cqc_providers_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Joins each location's latest provider name onto the coverage data.

    Deduplicates the providers data to the latest import date per provider before
    joining, since the raw providers data contains multiple rows per provider.

    Args:
        merged_coverage_lf (pl.LazyFrame): Coverage data so far.
        cqc_providers_lf (pl.LazyFrame): CQC providers data (may contain multiple
            rows per provider).

    Returns:
        pl.LazyFrame: Coverage data with the provider name added.
    """
    # TODO: dedupe cqc_providers_lf on provider_id, preferring the latest
    # cqc_provider_import_date, then left-join the provider name onto
    # merged_coverage_lf on provider_id, preserving the existing column order.
    return merged_coverage_lf
