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

_DOWNLOAD_TABLE_GROUP_COLUMNS: list[str] = [
    IndCQC.current_region,
    IndCQC.primary_service_type,
]
_DOWNLOAD_TABLE_SORT_COLUMNS: list[str] = [
    Pub.period,
    Pub.region,
    Pub.main_service,
]

# Maps the raw primary_service_type category values to their published
# display labels for the data-download tables - the rollup rows already
# hold their published labels directly (see add_rows_for_publication_groups).
_PUBLISHED_MAIN_SERVICE_BY_CATEGORY: dict[str, str] = {
    PrimaryServiceType.care_home_with_nursing: PublishedMainService.care_home_with_nursing,
    PrimaryServiceType.care_home_only: PublishedMainService.care_home_only,
    PrimaryServiceType.non_residential: PublishedMainService.non_residential,
}


def reduced_data_filter_expr(
    today: date | None = None,
    fy_start_month: int = 4,
    lookback_fy_years: int = 2,
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


def format_large_number(column_name: str, column_alias: str) -> pl.Expr:
    """
    Builds a polars expression formatting a number for display.

    Values at or above 1,000,000 are abbreviated to millions with three
    decimal places and an "m" suffix (e.g. 1175000 -> "1.175m", and
    2000000 -> "2.000m" rather than "2,000,000", so round-million values
    are formatted consistently with the rest of that bucket). Values below
    1,000,000 keep their full value with comma thousands separators (e.g.
    800000 -> "800,000"). Both branches round to the nearest whole number
    first, since fractional posts aren't meaningful for display.

    Args:
        column_name (str): the numeric column to format.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: string expression with the formatted value.
    """
    thousandths_of_millions = (
        (pl.col(column_name) / 1_000_000 * 1000).round(0).cast(pl.Int64)
    )
    millions_format = (
        (thousandths_of_millions // 1000).cast(pl.Utf8)
        + "."
        + (thousandths_of_millions % 1000).cast(pl.Utf8).str.zfill(3)
        + "m"
    )

    # Groups every 3 digits from the right (via a reverse/group/reverse round trip,
    # since polars' regex engine doesn't support the lookahead a single-pass
    # left-to-right version would need), then drops the comma this leaves
    # trailing whenever the digit count is itself a multiple of 3.
    rounded_whole = pl.col(column_name).round(0).cast(pl.Int64).cast(pl.Utf8)
    comma_format = (
        rounded_whole.str.reverse()
        .str.replace_all(r"(\d{3})", r"$1,")
        .str.strip_chars_end(",")
        .str.reverse()
    )

    return (
        pl.when(pl.col(column_name) >= 1_000_000)
        .then(millions_format)
        .otherwise(comma_format)
        .alias(column_alias)
    )


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

    Rounds to the nearest: 0 below 10; 10 from 10-14; 25 from 15-499; 50
    from 500-999; 100 from 1,000-9,999; 500 from 10,000-24,999; 1,000 from
    25,000-249,999; 5,000 from 250,000-1,499,999; 10,000 from 1,500,000
    upwards. Assumes a non-negative input, since every value this is
    applied to (a sum of filled posts or locations) is always >= 0.

    Args:
        column_name (str): the numeric column to round.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: float expression with the banded-rounded value.
    """
    value = pl.col(column_name)
    return (
        pl.when(value < 10)
        .then(0.0)
        .when(value < 15)
        .then(_round_half_away_from_zero(value, 10))
        .when(value < 500)
        .then(_round_half_away_from_zero(value, 25))
        .when(value < 1_000)
        .then(_round_half_away_from_zero(value, 50))
        .when(value < 10_000)
        .then(_round_half_away_from_zero(value, 100))
        .when(value < 25_000)
        .then(_round_half_away_from_zero(value, 500))
        .when(value < 250_000)
        .then(_round_half_away_from_zero(value, 1_000))
        .when(value < 1_500_000)
        .then(_round_half_away_from_zero(value, 5_000))
        .otherwise(_round_half_away_from_zero(value, 10_000))
        .alias(column_alias)
    )


def format_percentage(column_name: str, column_alias: str) -> pl.Expr:
    """
    Builds a polars expression formatting a percentage-change fraction (e.g.
    0.032 = +3.2%) to a 1 decimal place display string (e.g. "3.2%",
    "-0.8%"), for parity with the manually-published reference's own 1dp
    percentages. Null stays null, rather than becoming the string "null%" -
    the annual/monthly percentage change columns this is applied to are
    deliberately null on every row outside their own series.

    Args:
        column_name (str): the fractional percentage column to format.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: string expression with the formatted percentage, or null.
    """
    value = pl.col(column_name)
    rounded_percentage = _round_half_away_from_zero(value * 100, 0.1).round(1)
    return (
        pl.when(value.is_null())
        .then(None)
        .otherwise(rounded_percentage.cast(pl.Utf8) + "%")
        .alias(column_alias)
    )


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


def aggregate_to_publication_rows(
    lazy_df: pl.LazyFrame, group_keys: list[str] | None = None
) -> pl.LazyFrame:
    """
    Aggregates location-level rows up to publication level.

    Publication columns sum filled posts and count distinct locations across
    every row in a group. Assessment columns do the same but only over rows
    passing that term's consistent_service, dispersion and has-data filters,
    applied independently per term within the same group_by so a row can
    contribute to one term's assessment columns without contributing to
    another's.

    Capacity tracker (CT) values are location-level and repeat on each of a
    location's job role rows, so CT totals sum one row per location per group.
    The term filters are location-level, so the first row is representative.

    group_keys defaults to (import date, job role, region, service type). A
    narrower set (e.g. dropping job role) still counts each location once,
    since location counts and CT totals are recomputed at that grain rather
    than summed from separately-computed per-job-role values.

    Args:
        lazy_df (pl.LazyFrame): location-level data with consistent_service,
            ct_total_employed_imputed, ct_has_data_*_term and
            ct_dispersion_filter_*_term already added.
        group_keys (list[str] | None): columns to group by. Defaults to
            import date, job role, region and service type.

    Returns:
        pl.LazyFrame: one row per group_keys combination, with publication_*
            and assessment_*_term columns.
    """
    if group_keys is None:
        group_keys = [
            IndCQC.cqc_location_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]

    long_term_filter = (
        pl.col(Pub.consistent_service)
        & pl.col(Pub.ct_dispersion_filter_long_term)
        & pl.col(Pub.ct_has_data_long_term)
    )
    medium_term_filter = (
        pl.col(Pub.consistent_service)
        & pl.col(Pub.ct_dispersion_filter_medium_term)
        & pl.col(Pub.ct_has_data_medium_term)
    )
    short_term_filter = (
        pl.col(Pub.consistent_service)
        & pl.col(Pub.ct_dispersion_filter_short_term)
        & pl.col(Pub.ct_has_data_short_term)
    )

    filled_posts_col = IndCQC.estimate_filled_posts_by_job_role
    first_row_per_location = pl.col(IndCQC.location_id).is_first_distinct()

    return lazy_df.group_by(group_keys).agg(
        pl.col(filled_posts_col).sum().alias(Pub.publication_filled_posts),
        pl.col(IndCQC.location_id).n_unique().alias(Pub.publication_locationid_count),
        pl.col(filled_posts_col)
        .filter(long_term_filter)
        .sum()
        .alias(Pub.assessment_filled_posts_long_term),
        pl.col(IndCQC.location_id)
        .filter(long_term_filter)
        .n_unique()
        .alias(Pub.assessment_locationid_count_long_term),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(long_term_filter & first_row_per_location)
        .sum()
        .alias(Pub.assessment_ct_total_employed_long_term),
        pl.col(filled_posts_col)
        .filter(medium_term_filter)
        .sum()
        .alias(Pub.assessment_filled_posts_medium_term),
        pl.col(IndCQC.location_id)
        .filter(medium_term_filter)
        .n_unique()
        .alias(Pub.assessment_locationid_count_medium_term),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(medium_term_filter & first_row_per_location)
        .sum()
        .alias(Pub.assessment_ct_total_employed_medium_term),
        pl.col(filled_posts_col)
        .filter(short_term_filter)
        .sum()
        .alias(Pub.assessment_filled_posts_short_term),
        pl.col(IndCQC.location_id)
        .filter(short_term_filter)
        .n_unique()
        .alias(Pub.assessment_locationid_count_short_term),
        pl.col(Pub.ct_total_employed_imputed)
        .filter(short_term_filter & first_row_per_location)
        .sum()
        .alias(Pub.assessment_ct_total_employed_short_term),
    )


def add_rows_for_publication_groups(
    cleaned_lf: pl.LazyFrame, publication_summary_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Adds "All job roles", "All CQC care homes", "All CQC locations" and
    "England" rollup rows for the Tableau/Excel filter dropdowns.

    Built recursively - job role, then service type, then region - so
    England ends up with a row for every job role/service type combination,
    including the other rollups.

    Job role is rebuilt from cleaned_lf via aggregate_to_publication_rows
    with job role dropped from the group keys, since a location has one row
    per job role it employs and summing per-job-role counts would
    double-count it. Region and service type are single-valued per
    location, so those rollups just re-sum the aggregated columns.

    primary_service_type is a closed Enum in production, so it is cast to
    Categorical here, local to this job, before the new labels are written
    into it - the shared PrimaryServiceType enum is left alone, since it's
    also used by the IND CQC pipeline's own category-count validation.

    Args:
        cleaned_lf (pl.LazyFrame): location-level data, as passed into
            aggregate_to_publication_rows.
        publication_summary_lf (pl.LazyFrame): output of
            aggregate_to_publication_rows on cleaned_lf.

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

    metric_columns = [
        Pub.publication_filled_posts,
        Pub.publication_locationid_count,
        Pub.assessment_filled_posts_long_term,
        Pub.assessment_locationid_count_long_term,
        Pub.assessment_ct_total_employed_long_term,
        Pub.assessment_filled_posts_medium_term,
        Pub.assessment_locationid_count_medium_term,
        Pub.assessment_ct_total_employed_medium_term,
        Pub.assessment_filled_posts_short_term,
        Pub.assessment_locationid_count_short_term,
        Pub.assessment_ct_total_employed_short_term,
    ]

    all_job_roles_lf = (
        aggregate_to_publication_rows(
            cleaned_lf,
            group_keys=[
                IndCQC.cqc_location_import_date,
                IndCQC.current_region,
                IndCQC.primary_service_type,
            ],
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


def calc_perc_change_between_rows(
    column_name: str,
    from_date: date,
    group_columns: list[str],
    column_alias: str,
) -> pl.Expr:
    """
    Row-over-row percentage change in column_name within each group, as a
    net change fraction: (current - previous) / previous, so 0.25 = +25%.

    Restricted to periods on or after from_date: earlier periods are null,
    and so is a group's first in-window period, since a term's window
    never borrows a value from before its own start. "Previous" is the
    last present period in the group's data, not necessarily the prior
    calendar date, so a missing row spans the gap silently. Null (not
    inf/NaN) when the previous value is exactly 0, since column_name is a
    sum over a filter and can legitimately be 0.

    Args:
        column_name (str): the value column to measure change in.
        from_date (date): the earliest import date this term's window covers.
        group_columns (list[str]): columns identifying a group, e.g. region,
            job role and service type.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: float expression with the row-over-row percentage change.
    """
    in_window_value = (
        pl.when(pl.col(IndCQC.cqc_location_import_date) >= from_date)
        .then(pl.col(column_name))
        .otherwise(None)
    )
    previous_value = in_window_value.shift(1)
    return (
        pl.when(previous_value == 0)
        .then(None)
        .otherwise((in_window_value - previous_value) / previous_value)
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
    start. Null (not inf/NaN) when the baseline is exactly 0 - see
    calc_perc_change_between_rows for why column_name can legitimately be 0.
    column_name is cast to Float32 before dividing, so an integer count
    column (e.g. a location count) doesn't widen the result to Float64.

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
        .then(pl.col(column_name).cast(pl.Float32))
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


def calc_perc_change_against_periods_ago(
    column_name: str,
    periods_back: int,
    group_columns: list[str],
    column_alias: str,
) -> pl.Expr:
    """
    Percentage change in column_name against the row periods_back periods
    earlier within each group, as a net change fraction: (current -
    previous) / previous, so 0.25 = +25%.

    Unlike calc_perc_change_between_rows, this has no from_date window - the
    download tables compare across a group's whole history rather than a
    term's assessment window. "periods_back periods earlier" is the row that
    many places earlier in the group's data ordered by import date, not
    necessarily that many calendar periods back, so a missing row shifts the
    comparison silently. Null for a group's first periods_back rows, and
    null (not inf/NaN) when the earlier value is exactly 0 or missing, since
    column_name is a sum and can legitimately be 0. column_name is cast to
    Float32 before dividing, so an integer count column (e.g. a location
    count) doesn't widen the result to Float64.

    Args:
        column_name (str): the value column to measure change in.
        periods_back (int): how many rows earlier, within the group, to
            compare against (e.g. 1 for month-over-month, 12 for
            year-over-year on monthly data).
        group_columns (list[str]): columns identifying a group, e.g. region
            and service type.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: float expression with the lagged percentage change.
    """
    current_value = pl.col(column_name).cast(pl.Float32)
    previous_value = current_value.shift(periods_back).over(
        group_columns, order_by=IndCQC.cqc_location_import_date
    )
    return (
        pl.when(previous_value.is_null() | (previous_value == 0))
        .then(None)
        .otherwise((current_value - previous_value) / previous_value)
        .alias(column_alias)
    )


def _filter_to_all_job_roles_rollup(
    publication_summary_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    return publication_summary_lf.filter(
        pl.col(IndCQC.main_job_role_clean_labelled)
        == PublishedJobGroupLabels.all_job_roles
    )


def _period_label_expr() -> pl.Expr:
    """
    Builds a polars expression for a short display label of the period
    column, one month behind the raw date (e.g. 2026-04-01 -> "Mar-26").

    The download tables label a row by the financial year it closes out,
    not its own snapshot date: a row dated at a financial-year-start month
    (e.g. April) represents the year ending the month before, so the label
    is offset back by one month for display while the period column itself
    keeps the real underlying date.
    """
    return (
        pl.col(IndCQC.cqc_location_import_date)
        .dt.offset_by("-1mo")
        .dt.strftime("%b-%y")
        .alias(Pub.period_label)
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


def _annual_sampling_filter_expr(today: date | None, fy_start_month: int) -> pl.Expr:
    """
    Builds a polars expression reducing historical rows to one per year,
    keeping every row from the current financial year's start onwards in
    full, for the data-download tables.

    Reuses reduced_data_filter_expr with lookback_fy_years=0 (so the full
    retention window is the current financial year alone) and
    quarter_months=(fy_start_month,) (so the one historical month kept each
    year is the financial-year-start month itself, e.g. April - the row
    _period_label_expr then labels as the prior financial year's year-end,
    e.g. "Mar-26").

    Args:
        today (date | None): reference date. Defaults to the current system
            date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.Expr: boolean expression usable inside a `.filter()`.
    """
    return reduced_data_filter_expr(
        today=today,
        fy_start_month=fy_start_month,
        lookback_fy_years=0,
        quarter_months=(fy_start_month,),
    )


def _split_annual_and_monthly_perc_change(
    lazy_df: pl.LazyFrame,
    column_name: str,
    today: date | None,
    fy_start_month: int,
) -> pl.LazyFrame:
    """
    Adds annual_percentage_change and monthly_percentage_change to an
    annual-sampling-filtered frame.

    annual_percentage_change is a row-over-row change (the annual series is
    exactly one year apart row-to-row). monthly_percentage_change is
    cumulative against the financial-year-start baseline (the last annual
    row), not month-over-month - matching the published reference's own
    convention. The first monthly row's change is the same under either
    definition, since it's a single step from that baseline either way.
    Whichever period a row falls into gets that change, the other column
    stays null, since a row is never in both series.

    Args:
        lazy_df (pl.LazyFrame): output of _annual_sampling_filter_expr,
            filtered to the "All job roles" rollup.
        column_name (str): the value column to measure change in.
        today (date | None): reference date for the financial year split.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.LazyFrame: lazy_df with annual_percentage_change and
            monthly_percentage_change added.
    """
    fy_start = _financial_year_start(today, fy_start_month)
    raw_annual_change_column = "_raw_annual_perc_change"
    raw_monthly_change_column = "_raw_monthly_perc_change"
    lazy_df = lazy_df.with_columns(
        calc_perc_change_against_periods_ago(
            column_name,
            1,
            _DOWNLOAD_TABLE_GROUP_COLUMNS,
            raw_annual_change_column,
        ),
        calc_perc_change_cumulative_from_given_period_onwards(
            column_name,
            fy_start,
            _DOWNLOAD_TABLE_GROUP_COLUMNS,
            raw_monthly_change_column,
        ),
    )
    is_monthly_row = pl.col(IndCQC.cqc_location_import_date) > fy_start
    return lazy_df.with_columns(
        pl.when(is_monthly_row)
        .then(None)
        .otherwise(pl.col(raw_annual_change_column))
        .alias(Pub.annual_percentage_change),
        pl.when(is_monthly_row)
        .then(pl.col(raw_monthly_change_column))
        .otherwise(None)
        .alias(Pub.monthly_percentage_change),
    ).drop(raw_annual_change_column, raw_monthly_change_column)


def build_t0_estimates_download_table(
    publication_summary_lf: pl.LazyFrame,
    today: date | None = None,
    fy_start_month: int = 4,
) -> pl.LazyFrame:
    """
    Builds the T0 data-download table: estimated filled posts and CQC
    location count, one row per period, region and main service.

    Filters to the "All job roles" rollup row, since the download has no
    job-role breakdown, reduces historical periods to one per year with
    full monthly detail for the current financial year (see
    _annual_sampling_filter_expr), then renames the publication-level
    totals to the download's output column names.

    Args:
        publication_summary_lf (pl.LazyFrame): output of
            add_rows_for_publication_groups.
        today (date | None): reference date for the financial year split.
            Defaults to the current system date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.LazyFrame: one row per (period, region, main_service), with a
            period_label display column alongside the raw period date, and
            an estimated_filled_posts_formatted column holding the
            publication's banded-rounded figure (see
            round_to_publication_bands) alongside the raw value - cqc_locations
            is not rounded, since it already matches the published figures
            exactly as a plain count. Sorted by period then region then
            main_service.
    """
    return (
        _filter_to_all_job_roles_rollup(publication_summary_lf)
        .filter(_annual_sampling_filter_expr(today, fy_start_month))
        .select(
            pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
            _period_label_expr(),
            pl.col(IndCQC.current_region).alias(Pub.region),
            pl.col(IndCQC.primary_service_type)
            .cast(pl.Utf8)
            .replace(_PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
            .alias(Pub.main_service),
            pl.col(Pub.publication_filled_posts).alias(Pub.estimated_filled_posts),
            round_to_publication_bands(
                Pub.publication_filled_posts, Pub.estimated_filled_posts_formatted
            ),
            pl.col(Pub.publication_locationid_count).alias(Pub.cqc_locations),
        )
        .sort(_DOWNLOAD_TABLE_SORT_COLUMNS)
    )


def build_t1_filled_posts_perc_change_download_table(
    publication_summary_lf: pl.LazyFrame,
    today: date | None = None,
    fy_start_month: int = 4,
) -> pl.LazyFrame:
    """
    Builds the T1 data-download table: annual and monthly percentage change
    of estimated filled posts, one row per period, region and main service.

    Filters to the "All job roles" rollup row, since the download has no
    job-role breakdown, reduces historical periods to one per year with
    full monthly detail for the current financial year (see
    _annual_sampling_filter_expr), then computes the annual/monthly
    percentage change split against publication_filled_posts within each
    (region, main_service) group (see _split_annual_and_monthly_perc_change).

    Args:
        publication_summary_lf (pl.LazyFrame): output of
            add_rows_for_publication_groups.
        today (date | None): reference date for the financial year split.
            Defaults to the current system date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.LazyFrame: one row per (period, region, main_service), with a
            period_label display column alongside the raw period date, and
            a 1dp formatted display string (see format_percentage)
            alongside each raw percentage change value. Sorted by period
            then region then main_service.
    """
    all_job_roles_lf = _filter_to_all_job_roles_rollup(publication_summary_lf).filter(
        _annual_sampling_filter_expr(today, fy_start_month)
    )
    all_job_roles_lf = _split_annual_and_monthly_perc_change(
        all_job_roles_lf, Pub.publication_filled_posts, today, fy_start_month
    )
    return all_job_roles_lf.select(
        pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
        _period_label_expr(),
        pl.col(IndCQC.current_region).alias(Pub.region),
        pl.col(IndCQC.primary_service_type)
        .cast(pl.Utf8)
        .replace(_PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
        .alias(Pub.main_service),
        pl.col(Pub.annual_percentage_change),
        format_percentage(
            Pub.annual_percentage_change, Pub.annual_percentage_change_formatted
        ),
        pl.col(Pub.monthly_percentage_change),
        format_percentage(
            Pub.monthly_percentage_change, Pub.monthly_percentage_change_formatted
        ),
    ).sort(_DOWNLOAD_TABLE_SORT_COLUMNS)


def build_t2_location_count_perc_change_download_table(
    publication_summary_lf: pl.LazyFrame,
    today: date | None = None,
    fy_start_month: int = 4,
) -> pl.LazyFrame:
    """
    Builds the T2 data-download table: annual and monthly percentage change
    of CQC location count, one row per period, region and main service.

    Filters to the "All job roles" rollup row, since the download has no
    job-role breakdown, reduces historical periods to one per year with
    full monthly detail for the current financial year (see
    _annual_sampling_filter_expr), then computes the annual/monthly
    percentage change split against publication_locationid_count within
    each (region, main_service) group (see
    _split_annual_and_monthly_perc_change).

    Args:
        publication_summary_lf (pl.LazyFrame): output of
            add_rows_for_publication_groups.
        today (date | None): reference date for the financial year split.
            Defaults to the current system date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.LazyFrame: one row per (period, region, main_service), with a
            period_label display column alongside the raw period date, and
            a 1dp formatted display string (see format_percentage)
            alongside each raw percentage change value. Sorted by period
            then region then main_service.
    """
    all_job_roles_lf = _filter_to_all_job_roles_rollup(publication_summary_lf).filter(
        _annual_sampling_filter_expr(today, fy_start_month)
    )
    all_job_roles_lf = _split_annual_and_monthly_perc_change(
        all_job_roles_lf, Pub.publication_locationid_count, today, fy_start_month
    )
    return all_job_roles_lf.select(
        pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
        _period_label_expr(),
        pl.col(IndCQC.current_region).alias(Pub.region),
        pl.col(IndCQC.primary_service_type)
        .cast(pl.Utf8)
        .replace(_PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
        .alias(Pub.main_service),
        pl.col(Pub.annual_percentage_change),
        format_percentage(
            Pub.annual_percentage_change, Pub.annual_percentage_change_formatted
        ),
        pl.col(Pub.monthly_percentage_change),
        format_percentage(
            Pub.monthly_percentage_change, Pub.monthly_percentage_change_formatted
        ),
    ).sort(_DOWNLOAD_TABLE_SORT_COLUMNS)


def _build_base_estimate_rollup_lf(cleaned_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Builds one row per (period, region, primary_service_type - including the
    region/service/England rollups), summing estimate_filled_posts (the
    location-level, pre-job-role-split estimate) instead of the
    job-role-summed publication_filled_posts - shared by the T0 and T1
    verification tables, which diagnose whether a difference against a
    published reference originates in the job-role estimates
    split/reallocation rather than in this aggregation.

    Deduplicates to one row per (location_id, cqc_location_import_date)
    first: estimate_filled_posts repeats identically across every job role
    row for a location, so summing without deduplicating would multiply it
    by the location's job role count.

    Args:
        cleaned_lf (pl.LazyFrame): location-level data, as passed into
            aggregate_to_publication_rows. Must carry estimate_filled_posts
            (not selected by _01_merge.py by default - added there for this
            verification).

    Returns:
        pl.LazyFrame: one row per (cqc_location_import_date, current_region,
            primary_service_type), with estimated_filled_posts and
            cqc_locations columns, not yet period-sampled/selected/sorted.
    """
    location_level_lf = cleaned_lf.unique(
        subset=[IndCQC.location_id, IndCQC.cqc_location_import_date],
        keep="first",
    ).with_columns(pl.col(IndCQC.primary_service_type).cast(pl.Categorical))

    group_keys = [
        IndCQC.cqc_location_import_date,
        IndCQC.current_region,
        IndCQC.primary_service_type,
    ]
    all_locations_lf = location_level_lf.group_by(group_keys).agg(
        pl.col(IndCQC.estimate_filled_posts).sum().alias(Pub.estimated_filled_posts),
        pl.col(IndCQC.location_id).n_unique().alias(Pub.cqc_locations),
    )

    column_schema = all_locations_lf.collect_schema()
    column_order = column_schema.names()
    metric_columns = [Pub.estimated_filled_posts, Pub.cqc_locations]

    service_type_group_keys = [IndCQC.cqc_location_import_date, IndCQC.current_region]
    all_cqc_locations_lf = (
        all_locations_lf.group_by(service_type_group_keys)
        .agg([pl.col(column).sum() for column in metric_columns])
        .with_columns(
            pl.lit(PublishedMainService.all_locations)
            .cast(column_schema[IndCQC.primary_service_type])
            .alias(IndCQC.primary_service_type)
        )
        .select(column_order)
    )
    all_cqc_care_homes_lf = (
        all_locations_lf.filter(
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
        [all_locations_lf, all_cqc_locations_lf, all_cqc_care_homes_lf],
        how="vertical",
    )

    england_group_keys = [IndCQC.cqc_location_import_date, IndCQC.primary_service_type]
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


def build_t0_estimates_verification_table(
    cleaned_lf: pl.LazyFrame,
    today: date | None = None,
    fy_start_month: int = 4,
) -> pl.LazyFrame:
    """
    Builds a diagnostic table in T0's shape, but summing estimate_filled_posts
    (the location-level, pre-job-role-split estimate) instead of the
    job-role-summed publication_filled_posts - for verifying whether a
    difference against a published reference originates in the job-role
    estimates split/reallocation rather than in this aggregation.

    Args:
        cleaned_lf (pl.LazyFrame): location-level data, as passed into
            aggregate_to_publication_rows. Must carry estimate_filled_posts
            (not selected by _01_merge.py by default - added there for this
            verification).
        today (date | None): reference date for the financial year split.
            Defaults to the current system date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.LazyFrame: one row per (period, region, main_service), with a
            period_label display column alongside the raw period date, and
            an estimated_filled_posts_formatted column holding the
            publication's banded-rounded figure (see
            round_to_publication_bands) alongside the raw value. Sorted by
            period then region then main_service.
    """
    rollup_lf = _build_base_estimate_rollup_lf(cleaned_lf)

    return (
        rollup_lf.filter(_annual_sampling_filter_expr(today, fy_start_month))
        .select(
            pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
            _period_label_expr(),
            pl.col(IndCQC.current_region).alias(Pub.region),
            pl.col(IndCQC.primary_service_type)
            .cast(pl.Utf8)
            .replace(_PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
            .alias(Pub.main_service),
            pl.col(Pub.estimated_filled_posts),
            round_to_publication_bands(
                Pub.estimated_filled_posts, Pub.estimated_filled_posts_formatted
            ),
            pl.col(Pub.cqc_locations),
        )
        .sort(_DOWNLOAD_TABLE_SORT_COLUMNS)
    )


def build_t1_filled_posts_perc_change_verification_table(
    cleaned_lf: pl.LazyFrame,
    today: date | None = None,
    fy_start_month: int = 4,
) -> pl.LazyFrame:
    """
    Builds a diagnostic table in T1's shape, but computing percentage change
    against the location-level, pre-job-role-split estimate_filled_posts
    instead of the job-role-summed publication_filled_posts - for verifying
    whether a difference against a published reference originates in the
    job-role estimates split/reallocation rather than in this calculation.

    Args:
        cleaned_lf (pl.LazyFrame): location-level data, as passed into
            aggregate_to_publication_rows. Must carry estimate_filled_posts
            (not selected by _01_merge.py by default - added there for this
            verification).
        today (date | None): reference date for the financial year split.
            Defaults to the current system date when None.
        fy_start_month (int): month the financial year starts.

    Returns:
        pl.LazyFrame: one row per (period, region, main_service), with a
            period_label display column alongside the raw period date, and
            a 1dp formatted display string (see format_percentage)
            alongside each raw percentage change value. Sorted by period
            then region then main_service.
    """
    rollup_lf = _build_base_estimate_rollup_lf(cleaned_lf).filter(
        _annual_sampling_filter_expr(today, fy_start_month)
    )
    rollup_lf = _split_annual_and_monthly_perc_change(
        rollup_lf, Pub.estimated_filled_posts, today, fy_start_month
    )
    return rollup_lf.select(
        pl.col(IndCQC.cqc_location_import_date).alias(Pub.period),
        _period_label_expr(),
        pl.col(IndCQC.current_region).alias(Pub.region),
        pl.col(IndCQC.primary_service_type)
        .cast(pl.Utf8)
        .replace(_PUBLISHED_MAIN_SERVICE_BY_CATEGORY)
        .alias(Pub.main_service),
        pl.col(Pub.annual_percentage_change),
        format_percentage(
            Pub.annual_percentage_change, Pub.annual_percentage_change_formatted
        ),
        pl.col(Pub.monthly_percentage_change),
        format_percentage(
            Pub.monthly_percentage_change, Pub.monthly_percentage_change_formatted
        ),
    ).sort(_DOWNLOAD_TABLE_SORT_COLUMNS)
