from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

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
    values in the window, or where every value is zero.

    Args:
        column_name (str): the column to measure dispersion on.
        from_date (date): the earliest import date to measure from.

    Returns:
        pl.Expr: float expression, constant across all rows of a location.
    """
    values_per_import_date = (
        pl.struct(IndCQC.cqc_location_import_date, column_name)
        .filter(
            (pl.col(IndCQC.cqc_location_import_date) >= from_date)
            & pl.col(column_name).is_not_null()
        )
        .unique()
        .struct.field(column_name)
    )
    return (
        (
            (values_per_import_date.max() - values_per_import_date.min())
            / values_per_import_date.mean()
        )
        .over(IndCQC.location_id)
        .fill_nan(None)
    )


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


def aggregate_to_publication_rows():
    """
    Placeholder: Aggregate up to a row per import date, job role,
    primary_service_type and current_region.

    Sum filled posts and count count distinct locationid's.
    """
    pass


def add_rows_for_publication_groups():
    """
    Placeholder: Add rows for 'England', 'All CQC locations', 'All CQC care homes'
    and 'All job roles'
    """
    pass


def calc_perc_change_between_rows():
    """
    Placeholder: Add a column with the percentage change between rows of
    aggregated data.
    """
    pass


def calc_perc_change_cumulative_from_given_period_onwards():
    """
    Placeholder: Add a column with the cumulative percentage change from a given
    import date onwards
    """
    pass
