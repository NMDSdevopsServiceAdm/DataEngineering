import polars as pl

from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
)


def agency_and_non_agency_values_differ_filter() -> pl.Expr:
    """
    Filter expression for care home rows where agency and non-agency staff counts differ.

    Rows where all three job-role pairs match exactly are excluded elsewhere, as the
    likelihood of these numbers exactly matching is low and suggests poor data quality.
    Shared between the clean job (to filter rows) and the validate job (to compute the
    expected post-clean row count), so the two can't drift apart.

    Returns:
        pl.Expr: A boolean expression, True for rows to keep.
    """
    return (
        (pl.col(CTCH.nurses_employed) != pl.col(CTCH.agency_nurses_employed))
        | (
            pl.col(CTCH.care_workers_employed)
            != pl.col(CTCH.agency_care_workers_employed)
        )
        | (
            pl.col(CTCH.non_care_workers_employed)
            != pl.col(CTCH.agency_non_care_workers_employed)
        )
    )


def bound_columns(
    columns: list[str],
    lower_limit: int | None = None,
    upper_limit: int | None = None,
) -> pl.Expr:
    """
    Build an expression that nulls out-of-range values in the given columns, in place.

    Args:
        columns (list[str]): Columns to bound.
        lower_limit (int | None): Minimum accepted value, inclusive. No lower bound if None.
        upper_limit (int | None): Maximum accepted value, inclusive. No upper bound if None.

    Returns:
        pl.Expr: An expression bounding `columns`, nulling values outside the given range.
    """
    within_bounds = pl.lit(True)
    if lower_limit is not None:
        within_bounds &= pl.col(columns) >= lower_limit
    if upper_limit is not None:
        within_bounds &= pl.col(columns) <= upper_limit

    return pl.when(within_bounds).then(pl.col(columns)).otherwise(None).name.keep()
