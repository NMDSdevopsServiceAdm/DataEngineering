from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def has_column_data_since_date(
    column_name: str, from_date: date, column_alias: str
) -> pl.Expr:
    """
    Builds a per-location flag: does column_name have data since from_date?

    True for a location when it has at least one row on or after from_date,
    and column_name is not null on every one of those rows. A location with
    no rows at all on or after from_date is False, not vacuously True.

    Args:
        column_name (str): the column to check for nulls.
        from_date (date): the earliest import date to check from.
        column_alias (str): name to alias the resulting column to.

    Returns:
        pl.Expr: boolean expression aliased to column_alias, constant across
            all rows of a location.
    """
    in_window = pl.col(IndCQC.cqc_location_import_date) >= from_date
    has_row_in_window = in_window.any().over(IndCQC.location_id)
    no_nulls_in_window = ~(
        (in_window & pl.col(column_name).is_null()).any().over(IndCQC.location_id)
    )
    return (has_row_in_window & no_nulls_in_window).alias(column_alias)


def add_ct_filter_consistent_service() -> pl.Expr:
    """
    Placeholder: flags whether a location's service has been consistent.
    """
    pass


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
