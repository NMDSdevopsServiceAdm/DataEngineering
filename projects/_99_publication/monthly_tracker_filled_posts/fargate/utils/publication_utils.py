from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)

# A location is filtered out when its capacity tracker data swings further from
# the national average swing than this many standard deviations.
_DISPERSION_BOUNDARY_STD_DEVS: int = 2
_DISPERSION_COLUMN_SUFFIX: str = "_dispersion"

DOWNLOAD_TABLE_SORT_COLUMNS: list[str] = [
    Pub.period,
    Pub.region,
    Pub.main_service,
]

# The summed columns of the publication summary, and the extra ones added for
# the assessment (see aggregate_to_publication_rows).
_PUBLICATION_METRIC_COLUMNS: list[str] = [
    Pub.unfiltered_filled_posts,
    Pub.unfiltered_locationid_count,
]
_ASSESSMENT_METRIC_COLUMNS: list[str] = [
    Pub.filtered_long_sfc_filled_posts,
    Pub.filtered_long_locationid_count,
    Pub.filtered_long_ct_total_employed,
    Pub.filtered_medium_sfc_filled_posts,
    Pub.filtered_medium_locationid_count,
    Pub.filtered_medium_ct_total_employed,
    Pub.filtered_short_sfc_filled_posts,
    Pub.filtered_short_locationid_count,
    Pub.filtered_short_ct_total_employed,
    Pub.unfiltered_long_ct_total_employed,
]

# The publication's controlled-rounding bands (see round_to_publication_bands):
# round to `nearest` for any value below `upper_bound`, ascending. A value
# below _PUBLICATION_ROUNDING_SUPPRESSION_THRESHOLD is suppressed (null)
# rather than rounded.
_PUBLICATION_ROUNDING_SUPPRESSION_THRESHOLD: int = 10
_PUBLICATION_ROUNDING_BANDS: list[tuple[int, int]] = [
    (15, 10),
    (500, 25),
    (1_000, 50),
    (10_000, 100),
    (25_000, 500),
    (250_000, 1_000),
    (1_500_000, 5_000),
]
_PUBLICATION_ROUNDING_BAND_ABOVE_MAX: int = 10_000

# Published label for each primary_service_type category; the rollup rows
# already hold their published labels (see add_rows_for_publication_groups).
PUBLISHED_MAIN_SERVICE_BY_CATEGORY: dict[str, str] = {
    PrimaryServiceType.care_home_with_nursing: PublishedMainService.care_home_with_nursing,
    PrimaryServiceType.care_home_only: PublishedMainService.care_home_only,
    PrimaryServiceType.non_residential: PublishedMainService.non_residential,
}


def reduced_data_filter_expr(
    today: date | None = None,
    fy_start_month: int = 4,
    lookback_fy_years: int = 1,
    quarter_months: tuple[int, ...] = (1, 4, 7, 10),
    date_col: str = IndCQC.cqc_location_import_date,
    cutoff_date: date | None = None,
) -> pl.Expr:
    """
    Build a Polars expression for filtering a reduced dataset using financial-year
    windowing with quarterly sampling for older data.

    The filter implements a two-tier retention strategy:

    1. Full retention window:
       Rows with dates greater than or equal to the start of the current financial
       year minus `lookback_fy_years` are always included.

    2. Historical sampling window:
       Rows older than the full retention window are only included if their month
       falls within `quarter_months` (e.g. quarterly snapshots).

    This allows recent data to be fully retained while reducing storage and
    processing cost for older data via periodic sampling.

    Returning an expression rather than a filtered LazyFrame lets callers attach it
    directly to a `scan_parquet`, so the predicate is pushed down to the parquet
    source instead of running over a materialised frame.

    Args:
        today (date | None): Reference date used to compute financial year boundaries.
            If None, defaults to the current system date.

        fy_start_month (int): Month in which the financial year starts
            (default is 4 for April).

        lookback_fy_years (int): Number of financial years to retain in full before
            applying sampling.

        quarter_months (tuple[int, ...]): Months considered valid for quarterly sampling
            of historical data (Defaults to Jan, Apr, Jul, and Oct).

        date_col (str): Name of the date column the filter is applied to. Defaults to
            the CQC location import date; datasets keyed on a different date column
            pass their own.

        cutoff_date (date | None, optional): If given, acts as a hard floor: rows
            earlier than this date are always excluded, even if their month falls
            within `quarter_months`. Defaults to None (the historical sampling
            window is unbounded).

    Returns:
        pl.Expr: A Polars boolean expression that can be used inside `.filter()` or
            `.with_columns()` to select rows based on the reduced data strategy.
    """
    today: date = today or date.today()

    fy_year = today.year if today.month >= fy_start_month else today.year - 1

    monthly_start = date(fy_year - lookback_fy_years, fy_start_month, 1)

    dt = pl.col(date_col)

    expr = (dt >= monthly_start) | (
        (dt < monthly_start) & (dt.dt.month().is_in(quarter_months))
    )

    if cutoff_date is not None:
        expr = expr & (dt >= cutoff_date)

    return expr


def get_retention_cutoff_date(
    today: date | None = None,
    fy_start_month: int = 4,
    retention_fy_years: int = 6,
) -> date:
    """
    Gets the earliest date the publication retains: the start of the financial
    year `retention_fy_years` before the one containing today.

    Args:
        today (date | None): reference date. Defaults to the current system
            date when None.
        fy_start_month (int): month the financial year starts.
        retention_fy_years (int): number of financial years back to retain.

    Returns:
        date: the first day of the earliest financial year retained.
    """
    fy_start = _financial_year_start(today, fy_start_month)
    return fy_start.replace(year=fy_start.year - retention_fy_years)


def has_continuous_data_since_date(
    column_name: str, from_date: date, column_alias: str
) -> pl.Expr:
    """
    Builds a polars expression for flagging locations with data in
    column_name in all periods from from_date to latest import.

    True for a location when column_name is not null at every surviving
    sample date from from_date to the latest import date present in the
    data. A location missing a row entirely for one of those dates, or with
    a null value at one, is False — as is every location when there are no
    import dates on or after from_date at all. Counts distinct dates rather
    than rows, since a location can have multiple rows per import date (one
    per job role) with the same repeated column_name value.

    Args:
        column_name (str): the column to check for nulls.
        from_date (date): the earliest import date to check from.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: boolean expression aliased to column_alias, constant across
            all rows of a location.
    """
    in_window = pl.col(IndCQC.cqc_location_import_date) >= from_date
    dates_in_window = (
        pl.col(IndCQC.cqc_location_import_date).filter(in_window).n_unique()
    )
    non_null_dates_in_window = (
        pl.col(IndCQC.cqc_location_import_date)
        .filter(in_window & pl.col(column_name).is_not_null())
        .n_unique()
        .over(IndCQC.location_id)
    )
    return (
        (dates_in_window > 0) & (non_null_dates_in_window == dates_in_window)
    ).alias(column_alias)


def has_consistent_service_since_date(from_date: date, column_alias: str) -> pl.Expr:
    """
    Builds a polars expression for flagging locations whose care home status
    (from primary_service_type) is the same on every import date from
    from_date onwards. False when there are no rows on or after from_date.

    Args:
        from_date (date): the earliest import date to check from.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: boolean expression aliased to column_alias, constant across
            all rows of a location.
    """
    is_care_home = pl.col(IndCQC.primary_service_type).is_in(
        [PrimaryServiceType.care_home_with_nursing, PrimaryServiceType.care_home_only]
    )
    care_home_statuses_in_window = (
        is_care_home.filter(pl.col(IndCQC.cqc_location_import_date) >= from_date)
        .drop_nulls()
        .n_unique()
        .over(IndCQC.location_id)
    )
    return (care_home_statuses_in_window == 1).alias(column_alias)


def _round_half_away_from_zero(value: pl.Expr, nearest: float) -> pl.Expr:
    """
    Rounds value to the nearest multiple of nearest, rounding an exact half
    away from zero - matching Excel's ROUND, unlike polars' own `.round()`
    which rounds half to even. Needed for exact parity with a manually
    ROUND()-ed reference, where a half-to-even value would silently land in
    the wrong band/decimal on a tie.

    Normalises an exact-zero result to positive zero: the away-from-zero
    arithmetic can otherwise produce -0.0 for a negative input that rounds
    to zero, which would display as a stray minus sign.

    Args:
        value (pl.Expr): the numeric expression to round.
        nearest (float): the multiple to round to.

    Returns:
        pl.Expr: float expression rounded to the nearest multiple.
    """
    scaled = value / nearest
    rounded = (scaled.abs() + 0.5).floor() * scaled.sign() * nearest
    return pl.when(rounded == 0).then(0.0).otherwise(rounded)


def round_to_publication_bands(column_name: str, column_alias: str) -> pl.Expr:
    """
    Builds a polars expression applying the publication's controlled
    rounding to a non-negative figure (e.g. a summed filled posts total),
    for parity with the manually-published reference's own banded rounding.

    Rounds to the nearest: null (suppressed) below 10; 10 from 10-14; 25
    from 15-499; 50 from 500-999; 100 from 1,000-9,999; 500 from
    10,000-24,999; 1,000 from 25,000-249,999; 5,000 from 250,000-1,499,999;
    10,000 from 1,500,000 upwards - a small figure is nulled rather than
    rounded to 0, so it reads as suppressed rather than as a real zero.
    Assumes a non-negative input, since every value this is applied to (a
    sum of filled posts or locations) is always >= 0.

    Args:
        column_name (str): the numeric column to round.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: float expression with the banded-rounded value, or null
            for a value below the smallest band.
    """
    value = pl.col(column_name)
    expr = pl.when(value < _PUBLICATION_ROUNDING_SUPPRESSION_THRESHOLD).then(None)
    for upper_bound, nearest in _PUBLICATION_ROUNDING_BANDS:
        expr = expr.when(value < upper_bound).then(
            _round_half_away_from_zero(value, nearest)
        )
    return expr.otherwise(
        _round_half_away_from_zero(value, _PUBLICATION_ROUNDING_BAND_ABOVE_MAX)
    ).alias(column_alias)


def round_percentage(column_name: str) -> pl.Expr:
    """
    Builds a polars expression turning a percentage-change fraction into a
    percentage rounded to 1 decimal place (0.0321 -> 3.2), half away from
    zero, as a Float64. Null stays null.

    Args:
        column_name (str): the fractional percentage column.

    Returns:
        pl.Expr: float expression with the rounded percentage.
    """
    percentage = pl.col(column_name).cast(pl.Float64) * 100
    return _round_half_away_from_zero(percentage, 0.1).round(1)


def percentage_as_rounded_fraction(column_name: str) -> pl.Expr:
    """
    Rounds a fractional percentage change to one decimal place of a
    percentage (0.0321 -> 0.032) as a Float64 free of Float32 noise.

    Args:
        column_name (str): the fractional percentage change column.

    Returns:
        pl.Expr: the rounded fraction.
    """
    return (round_percentage(column_name) / 100).round(3)


def round_to_whole_number(column_name: str) -> pl.Expr:
    """
    Rounds a column to a whole number, an exact half away from zero (3.5 -> 4).
    Null stays null.

    Args:
        column_name (str): the column to round.

    Returns:
        pl.Expr: Int64 expression with the rounded value.
    """
    value = pl.col(column_name).cast(pl.Float64)
    return _round_half_away_from_zero(value, 1).cast(pl.Int64)


def add_dispersion_filter(
    lazy_df: pl.LazyFrame,
    column_names: list[str],
    from_date: date,
    column_alias: str,
) -> pl.LazyFrame:
    """
    Flags locations whose capacity tracker data is no more volatile than average.

    For each column in column_names a location's dispersion is
    (max - min) / mean of its values from from_date onwards, and the location
    passes when that dispersion sits within two standard deviations of the mean
    dispersion across all locations. The per-column results are coalesced, so a
    location is scored on whichever capacity tracker column it actually reports
    on, and a location with no data in the window at all is False.

    Each column is scored against its own population of locations rather than
    against a coalesced column, so a care home's volatility is only ever
    compared with other care homes' and a non-residential location's with other
    non-residential locations'.

    Dispersion is measured over distinct import dates, not rows: a location has
    one row per job role per import date and the capacity tracker columns are
    location-level, so the same value repeats across a date's job role rows.
    Averaging over rows would weight an import date by its job role count, which
    varies over time as job roles are added and reallocated. Max and min are
    unaffected by the repetition, but the mean is, so the (date, value) pairs
    are deduplicated before aggregating. For the same reason the national mean
    and standard deviation are taken over one dispersion value per location
    rather than over every row.

    Dispersion is written to a temporary column per source column rather than
    inlined as an expression. The boundary check references the dispersion five
    times, and as a single expression Polars re-evaluates the window each time -
    measured at 4.5x slower than materialising it once on a 2.4m row frame.

    Args:
        lazy_df (pl.LazyFrame): data to add the filter column to.
        column_names (list[str]): capacity tracker columns to measure
            dispersion on, in coalesce priority order.
        from_date (date): the earliest import date to measure from.
        column_alias (str): name to give the resulting boolean column.

    Returns:
        pl.LazyFrame: lazy_df with column_alias added, constant across all rows
            of a location.
    """
    dispersion_columns = [name + _DISPERSION_COLUMN_SUFFIX for name in column_names]

    lazy_df = lazy_df.with_columns(
        [
            _dispersion_since_date(column_name, from_date).alias(dispersion_column)
            for column_name, dispersion_column in zip(column_names, dispersion_columns)
        ]
    )
    lazy_df = lazy_df.with_columns(
        pl.coalesce(
            [
                _within_dispersion_boundaries(dispersion_column)
                for dispersion_column in dispersion_columns
            ]
        )
        .fill_null(False)
        .alias(column_alias)
    )

    return lazy_df.drop(dispersion_columns)


def _dispersion_since_date(column_name: str, from_date: date) -> pl.Expr:
    """
    Builds a polars expression for a location's dispersion in column_name.

    Deduplicates to one value per import date before aggregating, since a
    location has one row per job role per import date repeating the same
    location-level capacity tracker value. Null where the location has no
    values in the window, where every value is zero, or where the location
    has fewer than two distinct import dates with data in the window - a
    single observation always has a dispersion of exactly zero, which would
    silently collapse the national mean and standard deviation used as the
    boundary rather than being excluded like any other undefined case.

    Args:
        column_name (str): the column to measure dispersion on.
        from_date (date): the earliest import date to measure from.

    Returns:
        pl.Expr: float expression, constant across all rows of a location.
    """
    in_window_with_data = (
        pl.col(IndCQC.cqc_location_import_date) >= from_date
    ) & pl.col(column_name).is_not_null()

    values_per_import_date = (
        pl.struct(IndCQC.cqc_location_import_date, column_name)
        .filter(in_window_with_data)
        .unique()
        .struct.field(column_name)
    )
    dates_with_data = (
        pl.col(IndCQC.cqc_location_import_date)
        .filter(in_window_with_data)
        .n_unique()
        .over(IndCQC.location_id)
    )
    dispersion = (
        (values_per_import_date.max() - values_per_import_date.min())
        / values_per_import_date.mean()
    ).over(IndCQC.location_id)

    return pl.when(dates_with_data >= 2).then(dispersion).otherwise(None).fill_nan(None)


def _within_dispersion_boundaries(dispersion_column: str) -> pl.Expr:
    """
    Builds a polars expression for whether a dispersion is within boundaries.

    Takes the national mean and standard deviation over one dispersion value per
    location, not per row, since dispersion is already constant across a
    location's rows and row counts vary by location. Locations with a null
    dispersion are excluded from both, and stay null in the result so a caller
    can fall back to another column. The per-location values are cast to
    float64 for this step: at float32 precision, accumulated rounding error
    across the tens of thousands of locations behind this national mean and
    standard deviation could flip a location sitting close to the boundary.

    Args:
        dispersion_column (str): column holding a per-location dispersion.

    Returns:
        pl.Expr: boolean expression, null where dispersion_column is null.
    """
    dispersion_per_location = (
        pl.col(dispersion_column)
        .filter(pl.col(IndCQC.location_id).is_first_distinct())
        .cast(pl.Float64)
    )
    mean_dispersion = dispersion_per_location.mean()
    std_dispersion = dispersion_per_location.std()

    return pl.col(dispersion_column).is_between(
        mean_dispersion - _DISPERSION_BOUNDARY_STD_DEVS * std_dispersion,
        mean_dispersion + _DISPERSION_BOUNDARY_STD_DEVS * std_dispersion,
        closed="both",
    )


def _assessment_aggregations() -> list[pl.Expr]:
    """
    Builds the assessment aggregations for aggregate_to_publication_rows.

    Each term sums filled posts, counts distinct locations and sums capacity
    tracker (CT) employed, only over rows passing that term's consistent
    service, dispersion and has-data filters. CT values are location-level and
    repeat on each of a location's job role rows, so CT totals sum one row per
    location per group; the term filters are location-level, so the first row
    is representative. A final column sums CT employed across all locations
    with no term filters.

    Returns:
        list[pl.Expr]: aggregation expressions, one per assessment column.
    """
    long_term_filter = (
        pl.col(Pub.consistent_service_long)
        & pl.col(Pub.ct_dispersion_filter_long)
        & pl.col(Pub.ct_has_data_long)
    )
    medium_term_filter = (
        pl.col(Pub.consistent_service_medium)
        & pl.col(Pub.ct_dispersion_filter_medium)
        & pl.col(Pub.ct_has_data_medium)
    )
    short_term_filter = (
        pl.col(Pub.consistent_service_short)
        & pl.col(Pub.ct_dispersion_filter_short)
        & pl.col(Pub.ct_has_data_short)
    )

    filled_posts_col = IndCQC.estimate_filled_posts_by_job_role
    first_row_per_location = pl.col(IndCQC.location_id).is_first_distinct()

    return [
        pl.col(filled_posts_col)
        .filter(long_term_filter)
        .sum()
        .alias(Pub.filtered_long_sfc_filled_posts),
        pl.col(IndCQC.location_id)
        .filter(long_term_filter)
        .n_unique()
        .alias(Pub.filtered_long_locationid_count),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(long_term_filter & first_row_per_location)
        .sum()
        .alias(Pub.filtered_long_ct_total_employed),
        pl.col(filled_posts_col)
        .filter(medium_term_filter)
        .sum()
        .alias(Pub.filtered_medium_sfc_filled_posts),
        pl.col(IndCQC.location_id)
        .filter(medium_term_filter)
        .n_unique()
        .alias(Pub.filtered_medium_locationid_count),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(medium_term_filter & first_row_per_location)
        .sum()
        .alias(Pub.filtered_medium_ct_total_employed),
        pl.col(filled_posts_col)
        .filter(short_term_filter)
        .sum()
        .alias(Pub.filtered_short_sfc_filled_posts),
        pl.col(IndCQC.location_id)
        .filter(short_term_filter)
        .n_unique()
        .alias(Pub.filtered_short_locationid_count),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(short_term_filter & first_row_per_location)
        .sum()
        .alias(Pub.filtered_short_ct_total_employed),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(first_row_per_location)
        .sum()
        .alias(Pub.unfiltered_long_ct_total_employed),
    ]


def aggregate_to_publication_rows(
    lazy_df: pl.LazyFrame,
    group_keys: list[str] | None = None,
    include_assessment: bool = True,
) -> pl.LazyFrame:
    """
    Aggregates location-level rows up to publication level.

    Unfiltered columns sum filled posts and count distinct locations across
    every row in a group. Filtered columns (see _assessment_aggregations) do
    the same but only over rows passing each term's filters, applied
    independently per term within the same group_by so a row can contribute to
    one term's filtered columns without contributing to another's.

    group_keys defaults to (import date, period label, job role, region,
    service type); the period label is one-to-one with import date, so it
    adds no groups. A narrower set (e.g. dropping job role) still counts each
    location once, since location counts and CT totals are recomputed at that
    grain rather than summed from separately-computed per-job-role values.

    Args:
        lazy_df (pl.LazyFrame): location-level data. With include_assessment,
            it must also hold consistent_service_*,
            ct_total_employed_imputed, ct_has_data_* and
            ct_dispersion_filter_*.
        group_keys (list[str] | None): columns to group by. Defaults to
            import date, period label, job role, region and service type.
        include_assessment (bool): whether to add the assessment columns.

    Returns:
        pl.LazyFrame: one row per group_keys combination, with unfiltered
            columns and, with include_assessment, filtered_* columns.
    """
    if group_keys is None:
        group_keys = [
            IndCQC.cqc_location_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]

    aggregations = [
        pl.col(IndCQC.estimate_filled_posts_by_job_role)
        .sum()
        .alias(Pub.unfiltered_filled_posts),
        pl.col(IndCQC.location_id).n_unique().alias(Pub.unfiltered_locationid_count),
    ]
    if include_assessment:
        aggregations += _assessment_aggregations()

    return lazy_df.group_by(group_keys).agg(aggregations)


def add_rows_for_publication_groups(
    prepared_lf: pl.LazyFrame,
    publication_summary_lf: pl.LazyFrame,
    include_assessment: bool = True,
) -> pl.LazyFrame:
    """
    Adds "All job roles", "All CQC care homes", "All CQC locations" and
    "England" rollup rows for the Tableau/Excel filter dropdowns.

    Built recursively - job role, then service type, then region - so
    England has a row for every job role/service type combination,
    including the other rollups.

    Job role is rebuilt from prepared_lf via aggregate_to_publication_rows
    with job role dropped from the group keys, since summing per-job-role
    counts would double-count a location (one row per job role it employs).
    Region and service type are single-valued per location, so those
    rollups re-sum the aggregated columns.

    primary_service_type is a closed Enum in production, so it is cast to
    Categorical here before the new labels are written into it. The shared
    PrimaryServiceType enum is left alone: the IND CQC pipeline's
    category-count validation also uses it.

    The job role rows (small, post-aggregation) are collected so every
    rollup and sink reuses one evaluation of prepared_lf; left lazy, Polars
    re-runs the whole upstream plan per consumer.

    Args:
        prepared_lf (pl.LazyFrame): location-level data, as passed into
            aggregate_to_publication_rows.
        publication_summary_lf (pl.LazyFrame): output of
            aggregate_to_publication_rows on prepared_lf.
        include_assessment (bool): whether publication_summary_lf has the
            assessment columns, as passed to aggregate_to_publication_rows.

    Returns:
        pl.LazyFrame: publication_summary_lf with rollup rows added for
            "All job roles", "All CQC care homes", "All CQC locations" and
            "England".
    """
    publication_summary_lf = publication_summary_lf.with_columns(
        pl.col(IndCQC.primary_service_type).cast(pl.Categorical)
    )
    column_schema = publication_summary_lf.collect_schema()
    column_order = column_schema.names()

    metric_columns = _PUBLICATION_METRIC_COLUMNS + (
        _ASSESSMENT_METRIC_COLUMNS if include_assessment else []
    )

    all_job_roles_lf = (
        aggregate_to_publication_rows(
            prepared_lf,
            group_keys=[
                IndCQC.cqc_location_import_date,
                IndCQC.current_region,
                IndCQC.primary_service_type,
            ],
            include_assessment=include_assessment,
        )
        .with_columns(
            pl.lit(PublishedJobGroupLabels.all_job_roles)
            .cast(column_schema[IndCQC.main_job_role_clean_labelled])
            .alias(IndCQC.main_job_role_clean_labelled),
            pl.col(IndCQC.primary_service_type).cast(pl.Categorical),
        )
        .select(column_order)
    )
    job_role_enlarged_lf = pl.concat(
        [publication_summary_lf, all_job_roles_lf], how="vertical"
    )
    job_role_enlarged_lf = job_role_enlarged_lf.collect(engine="streaming").lazy()

    service_type_group_keys = [
        IndCQC.cqc_location_import_date,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.current_region,
    ]
    all_cqc_locations_lf = (
        job_role_enlarged_lf.group_by(service_type_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedMainService.all_locations)
            .cast(column_schema[IndCQC.primary_service_type])
            .alias(IndCQC.primary_service_type)
        )
        .select(column_order)
    )
    all_cqc_care_homes_lf = (
        job_role_enlarged_lf.filter(
            pl.col(IndCQC.primary_service_type).is_in(
                [
                    PrimaryServiceType.care_home_with_nursing,
                    PrimaryServiceType.care_home_only,
                ]
            )
        )
        .group_by(service_type_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedMainService.all_care_homes)
            .cast(column_schema[IndCQC.primary_service_type])
            .alias(IndCQC.primary_service_type)
        )
        .select(column_order)
    )
    service_type_enlarged_lf = pl.concat(
        [job_role_enlarged_lf, all_cqc_locations_lf, all_cqc_care_homes_lf],
        how="vertical",
    )

    england_group_keys = [
        IndCQC.cqc_location_import_date,
        IndCQC.main_job_role_clean_labelled,
        IndCQC.primary_service_type,
    ]
    england_lf = (
        service_type_enlarged_lf.group_by(england_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedRegion.england)
            .cast(column_schema[IndCQC.current_region])
            .alias(IndCQC.current_region)
        )
        .select(column_order)
    )

    return pl.concat([service_type_enlarged_lf, england_lf], how="vertical")


def calc_perc_change_from_previous_april(
    column_name: str,
    group_columns: list[str],
    column_alias: str,
) -> pl.Expr:
    """
    Percentage change in column_name on April rows against the group's
    previous April, as a net change fraction: (current - previous) / previous,
    so 0.25 = +25%.

    Null on non-April rows, a group's first April, and (not inf/NaN) when the
    previous April is 0. Calculated as Float64 to avoid rounding drift.

    Args:
        column_name (str): the value column to measure change in.
        group_columns (list[str]): columns identifying a group, e.g. region,
            job role and service type.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: float expression with the April-on-April percentage change.
    """
    april_value = (
        pl.when(pl.col(IndCQC.cqc_location_import_date).dt.month() == 4)
        .then(pl.col(column_name).cast(pl.Float64))
        .otherwise(None)
    )
    previous_april_value = april_value.forward_fill().shift(1)
    return (
        pl.when(previous_april_value == 0)
        .then(None)
        .otherwise((april_value - previous_april_value) / previous_april_value)
        .over(group_columns, order_by=IndCQC.cqc_location_import_date)
        .alias(column_alias)
    )


def calc_perc_change_cumulative_from_given_period_onwards(
    column_name: str,
    from_date: date,
    group_columns: list[str],
    column_alias: str,
) -> pl.Expr:
    """
    Cumulative percentage change in column_name within each group, as a net
    change fraction against the group's first in-window value (the
    baseline): (current - baseline) / baseline. The baseline period itself
    is 0.0.

    Restricted to periods on or after from_date: earlier periods are null,
    since a term's window never borrows a baseline from before its own
    start. Null (not inf/NaN) when the baseline is exactly 0, since column_name
    is a sum over a filter and can legitimately be 0.
    Calculated as Float64 to avoid rounding drift.

    Args:
        column_name (str): the value column to measure change in.
        from_date (date): the earliest import date this term's window covers.
        group_columns (list[str]): columns identifying a group, e.g. region,
            job role and service type.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: float expression with the cumulative percentage change.
    """
    in_window_value = (
        pl.when(pl.col(IndCQC.cqc_location_import_date) >= from_date)
        .then(pl.col(column_name).cast(pl.Float64))
        .otherwise(None)
    )
    baseline_value = in_window_value.filter(in_window_value.is_not_null()).first()
    return (
        pl.when(baseline_value == 0)
        .then(None)
        .otherwise((in_window_value - baseline_value) / baseline_value)
        .over(group_columns, order_by=IndCQC.cqc_location_import_date)
        .alias(column_alias)
    )


def add_prev_run_filled_posts_columns(
    assessment_summary_lf: pl.LazyFrame,
    prev_assessment_lf: pl.LazyFrame | None,
) -> pl.LazyFrame:
    """
    Adds the previous run's filled posts and the percentage change
    from it, matched on import date, job role, region and service type.

    The percentage change is a net change fraction, (current - previous) /
    previous, so 0.25 = +25%. Null (not inf/NaN) when previous is 0.

    Rows with no match are null, as is every row when there is no previous run
    or it lacks unfiltered_filled_posts. The previous keys are cast to the
    current dtypes so Enum/Categorical differences don't block the join.

    Args:
        assessment_summary_lf (pl.LazyFrame): the current run's assessment data.
        prev_assessment_lf (pl.LazyFrame | None): the previous run's assessment
            data, or None when there is no earlier run.

    Returns:
        pl.LazyFrame: assessment_summary_lf with unfiltered_filled_posts_prev_run and
            unfiltered_filled_posts_pct_change_vs_prev_run added.
    """
    current_schema = assessment_summary_lf.collect_schema()
    has_prev_filled_posts = (
        prev_assessment_lf is not None
        and Pub.unfiltered_filled_posts in prev_assessment_lf.collect_schema().names()
    )
    if has_prev_filled_posts:
        join_keys = [
            IndCQC.cqc_location_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]
        prev_assessment_lf = prev_assessment_lf.select(
            [pl.col(key).cast(current_schema[key]) for key in join_keys]
            + [
                pl.col(Pub.unfiltered_filled_posts)
                .cast(current_schema[Pub.unfiltered_filled_posts])
                .alias(Pub.unfiltered_filled_posts_prev_run)
            ]
        )
        assessment_summary_lf = assessment_summary_lf.join(
            prev_assessment_lf,
            on=join_keys,
            how="left",
            validate="1:1",
            nulls_equal=True,
        )
    else:
        assessment_summary_lf = assessment_summary_lf.with_columns(
            pl.lit(None, dtype=current_schema[Pub.unfiltered_filled_posts]).alias(
                Pub.unfiltered_filled_posts_prev_run
            )
        )

    current_posts = pl.col(Pub.unfiltered_filled_posts).cast(pl.Float64)
    prev_posts = pl.col(Pub.unfiltered_filled_posts_prev_run).cast(pl.Float64)
    return assessment_summary_lf.with_columns(
        pl.when(prev_posts == 0)
        .then(None)
        .otherwise((current_posts - prev_posts) / prev_posts)
        .alias(Pub.unfiltered_filled_posts_pct_change_vs_prev_run)
    )


def import_date_to_period_month(import_date: pl.Expr) -> pl.Expr:
    """
    Maps a CQC import date to the month it reports on: one month earlier
    (2026-04-01 -> 2026-03-01).

    Args:
        import_date (pl.Expr): expression of CQC import dates.

    Returns:
        pl.Expr: the period month.
    """
    return import_date.dt.offset_by("-1mo")


def period_label_short_expr() -> pl.Expr:
    """
    Builds the short display label of the period, one month behind the import
    date (e.g. 2026-04-01 -> "Mar 2026").

    Returns:
        pl.Expr: the label, aliased to the short period label column.
    """
    return (
        import_date_to_period_month(pl.col(IndCQC.cqc_location_import_date))
        .dt.strftime("%b %Y")
        .alias(Pub.period_label_short)
    )


def period_label_long_expr() -> pl.Expr:
    """
    Builds the long display label of the period, one month behind the import
    date (e.g. 2026-04-01 -> "March 2026").

    Returns:
        pl.Expr: the label, aliased to the long period label column.
    """
    return (
        import_date_to_period_month(pl.col(IndCQC.cqc_location_import_date))
        .dt.strftime("%B %Y")
        .alias(Pub.period_label_long)
    )


def _financial_year_start(today: date | None, fy_start_month: int) -> date:
    """
    The first day of the financial year containing today (e.g. 2026-10-06
    with fy_start_month=4 -> 2026-04-01).

    Args:
        today (date | None): reference date. Defaults to the current system
            date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        date: the first day of that financial year.
    """
    today = today or date.today()
    fy_year = today.year if today.month >= fy_start_month else today.year - 1
    return date(fy_year, fy_start_month, 1)
