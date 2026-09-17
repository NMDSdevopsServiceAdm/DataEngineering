from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

# A location is filtered out when its capacity tracker data swings further from
# the national average swing than this many standard deviations.
_DISPERSION_BOUNDARY_STD_DEVS: int = 2
_DISPERSION_COLUMN_SUFFIX: str = "_dispersion"


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


def aggregate_to_publication_rows(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Aggregates location-level rows up to publication level.

    Publication columns sum filled posts and count distinct locations across
    every row in a group. Assessment columns do the same but only over rows
    passing that term's consistent_service, dispersion and has-data filters,
    applied independently per term within the same group_by so a row can
    contribute to one term's assessment columns without contributing to
    another's.

    Args:
        lazy_df (pl.LazyFrame): location-level data with consistent_service,
            ct_total_employed_imputed, ct_has_data_*_term and
            ct_dispersion_filter_*_term already added.

    Returns:
        pl.LazyFrame: one row per (import date, job role, region, service
            type) group, with publication_* and assessment_*_term columns.
    """
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

    filled_posts_col = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated

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
        .filter(long_term_filter)
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
        .filter(medium_term_filter)
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
        .filter(short_term_filter)
        .sum()
        .alias(Pub.assessment_ct_total_employed_short_term),
    )


def add_rows_for_publication_groups():
    """
    Placeholder: Add rows for 'England', 'All CQC locations', 'All CQC care homes'
    and 'All job roles'
    """
    pass


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
        .then(pl.col(column_name))
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
