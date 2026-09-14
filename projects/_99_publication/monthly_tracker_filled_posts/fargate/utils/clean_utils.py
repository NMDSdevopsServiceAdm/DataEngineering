from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


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


def add_ct_filter_dispersion_filter() -> pl.Expr:
    """
    Placeholder: flags whether a location passes the dispersion filter.
    """
    pass


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
