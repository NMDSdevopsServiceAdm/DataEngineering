import polars as pl

from utils.column_names.publication_columns import PublicationColumns as Pub


def add_ct_filter_has_ct_data() -> pl.Expr:
    """
    Placeholder: flags whether a location has capacity tracker data.
    """
    pass


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
