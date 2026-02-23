import polars as pl

from dataclasses import dataclass, fields
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class TempCols:
    """The names of the temporary columns used in this process."""

    days_to_forward_fill: str = "days_to_forward_fill"
    last_known_date: str = "last_known_date"
    last_known_value: str = "last_known_value"
    latest: str = "latest"


# Dictionary defining the minimum size of location and corresponding days to forward fill.
SIZE_BASED_FORWARD_FILL_DAYS = {
    -float("inf"): 65,
}


def forward_fill_latest_known_value(
    lf: pl.LazyFrame, col_to_forward_fill: str
) -> pl.LazyFrame:
    """
    Forward-fills the last known value of a column for each location, using a
    length of time base on the location size.

    The length of time to forward-fill is determined by the
    SIZE_BASED_FORWARD_FILL_DAYS dictionary.

    This function has the following steps:
        1. Determine a size-based number of days to forward-fill for each row.
        2. Identify the latest non-null value and its date for each location.
        3. Forward-fill `col_to_forward_fill` for rows where:
            - the column value is null,
            - the import date is after the last known submission date, and
            - the date difference is within the limit defined by
              `days_to_forward_fill`.
        4. Drop intermediate helper columns.

    Args:
        lf (pl.LazyFrame): Input polars LazyFrame with nulls in
            `col_to_forward_fill` forward-filled according to the size-based length
            of time.
        col_to_forward_fill (str): Name of the column whose last known value will be propagated forward.

    Returns:
        pl.LazyFrame: The polars LazyFrame with null values in `col_to_repeat`
            replaced where appropriate.
    """
    last_known_lf = return_last_known_value(lf, col_to_forward_fill)

    last_known_lf = add_size_based_forward_fill_days(
        last_known_lf, TempCols.last_known_value, SIZE_BASED_FORWARD_FILL_DAYS
    )

    lf = lf.join(last_known_lf, on=IndCQC.location_id, how="left")

    forward_fill_lf = forward_fill(lf, col_to_forward_fill)

    columns_to_drop = [field.name for field in fields(TempCols())]
    forward_fill_lf = forward_fill_lf.drop(*columns_to_drop)

    return forward_fill_lf


def add_size_based_forward_fill_days(
    lf: pl.LazyFrame,
    location_size_col: str,
    size_based_forward_fill_days: dict[int, int],
) -> pl.LazyFrame:
    """
    Adds a column to the LazyFrame indicating the number of days to forward fill
    based on location size.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing `location_size` column.
        location_size_col (str): Column name for determining location size.
        size_based_forward_fill_days (dict[int, int]): Dictionary mapping
            minimum location sizes to days to forward fill.

    Returns:
        pl.LazyFrame: LazyFrame with an additional column indicating days to
            forward fill.
    """
    expr = None

    for min_size, days in sorted(size_based_forward_fill_days.items()):
        condition = pl.col(location_size_col) >= min_size
        if expr is None:
            expr = pl.when(condition).then(days)
        else:
            expr = pl.when(condition).then(days).otherwise(expr)

    return lf.with_columns(expr.alias(TempCols.days_to_forward_fill))


def return_last_known_value(
    lf: pl.LazyFrame,
    col_to_forward_fill: str,
) -> pl.LazyFrame:
    """
    This function gets the last known non-null value for col_to_forward_fill and
    the date for this value

    Args:
        lf (pl.LazyFrame): Input polars LazyFrame with column to repeat.
        col_to_forward_fill (str): Column for which we need the last known
            non-null value.

    Returns:
        pl.LazyFrame: A polars LazyFrame with last known non-null value and the
            date when it has the last non-null value.
    """
    lf_with_last_known = (
        lf.filter(pl.col(col_to_forward_fill).is_not_null())
        .sort(IndCQC.cqc_location_import_date)
        .group_by(IndCQC.location_id)
        .agg(
            [
                pl.col(IndCQC.cqc_location_import_date)
                .last()
                .alias(TempCols.last_known_date),
                pl.col(col_to_forward_fill).last().alias(TempCols.last_known_value),
            ]
        )
    )
    return lf_with_last_known


def forward_fill(lf: pl.LazyFrame, col_to_forward_fill: str) -> pl.LazyFrame:
    """
    Forward fill column to repeat based on certain conditions

    This function forward fills the column to repeat with the last known value
    for the number of days we want it to repeat. It checks if the import date
    and last known date is less than days_to_repeat, if it is it fills those
    with last known value calculated in previous function.

    Args:
        lf (pl.LazyFrame): Input polars LazyFrame with last know value and date.
        col_to_forward_fill (str): Column name which needs forward filling.

    Returns:
        pl.LazyFrame: A polars LazyFrame with forward filled column.
    """
    after_last_known_date_condition = pl.col(IndCQC.cqc_location_import_date) > pl.col(
        TempCols.last_known_date
    )

    within_days_to_fill_condition = (
        pl.col(IndCQC.cqc_location_import_date) - pl.col(TempCols.last_known_date)
    ).dt.total_days() <= pl.col(TempCols.days_to_forward_fill)

    forward_fill_condition = (
        pl.col(col_to_forward_fill).is_null()
        & after_last_known_date_condition
        & within_days_to_fill_condition
    )

    lf_with_forward_fill = lf.with_columns(
        pl.when(
            forward_fill_condition
            & after_last_known_date_condition
            & within_days_to_fill_condition
        )
        .then(pl.col(TempCols.last_known_value))
        .otherwise(pl.col(col_to_forward_fill))
        .alias(col_to_forward_fill),
    )

    return lf_with_forward_fill
