import polars as pl

from utils.column_names.publication_columns import PublicationColumns as Pub


def add_ct_filter_has_ct_data() -> pl.Expr:
    """
    Placeholder: flags whether a location has capacity tracker data.

    Returns:
        pl.Expr: a literal True, aliased to PublicationColumns.ct_has_data.
    """
    return pl.lit(True).alias(Pub.ct_has_data)


def add_ct_filter_consistent_service() -> pl.Expr:
    """
    Placeholder: flags whether a location's service has been consistent.

    Returns:
        pl.Expr: a literal True, aliased to PublicationColumns.consistent_service.
    """
    return pl.lit(True).alias(Pub.consistent_service)


def add_ct_filter_dispersion_filter() -> pl.Expr:
    """
    Placeholder: flags whether a location passes the dispersion filter.

    Returns:
        pl.Expr: a literal True, aliased to PublicationColumns.ct_dispersion_filter.
    """
    return pl.lit(True).alias(Pub.ct_dispersion_filter)


def split_into_assessment_and_publication_data(
    lf: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Placeholder: splits cleaned data into assessment and publication datasets.

    Args:
        lf (pl.LazyFrame): The cleaned job role data.

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: lf, returned unchanged as both the
            assessment and publication LazyFrame.
    """
    # Commented out to prevent duplicating the execution plan once other functions are written.
    # return lf, lf
