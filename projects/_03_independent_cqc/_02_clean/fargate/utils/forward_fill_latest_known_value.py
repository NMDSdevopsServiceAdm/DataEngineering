import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def forward_fill_latest_known_value(
    lf: pl.LazyFrame,
    col_to_repeat: str,
    days_to_repeat: int,
) -> pl.LazyFrame:
    """
    Copies the latest known value of col_to_repeat forward for the given days_to_repeat through cqc_location_import_date
    per locationid.

    A generalised function which allows a given column to be forward-filled by making the propagation window configurable.
    This avoids hardcoding logic and makes the function suitable for reuse with other datasets (e.g., PIR data) and
    with varying time windows as requirements change. This function has the following steps:
        1. Identify the latest non-null value for `col_to_repeat` per location and that values import date.
        2. Join this information back onto the main dataset.
        3. Forward-fill the column for rows where:
            - the column value is null,
            - the import date is after the last known submission date, and
            - the date difference does not exceed `days_to_repeat`.
        4. Drop intermediate helper columns.

    Args:
        lf (pl.LazyFrame): Input polars LazyFrame containing `location_id`, `cqc_location_import_date` and col_to_repeat.
        col_to_repeat (str): Name of the column whose last known value will be propagated forward.
        days_to_repeat (int): Maximum number of days after the last known submission for which the
                            value should be forward-filled.

    Returns:
        pl.LazyFrame: The polars LazyFrame with null values in `col_to_repeat` replaced where appropriate.
    """
    lf_with_last_known = return_last_known_value(lf, col_to_repeat)

    lf_with_forward_fill = forward_fill(
        lf_with_last_known, col_to_repeat, days_to_repeat
    )

    return lf_with_forward_fill


def return_last_known_value(
    lf: pl.LazyFrame,
    col_to_repeat: str,
) -> pl.LazyFrame:
    """
    This function gets the last known non-null value for col_to_repeat and the date for this value

    Args:
        lf (pl.LazyFrame): Input polars LazyFrame with column to repeat.
        col_to_repeat (str): Column for which we need the last known non-null value.

    Returns:
        pl.LazyFrame: A polars LazyFrame with last known non-null value and the date when it has the last non-null value.
    """
    last_known_date = "last_known_date"
    last_known_value = "last_known_value"

    lf = lf.with_columns(
        (
            pl.col(IndCQC.cqc_location_import_date)
            .filter(pl.col(col_to_repeat).is_not_null())
            .last()
            .over(
                partition_by=IndCQC.location_id,
                order_by=IndCQC.cqc_location_import_date,
            )
            .alias(last_known_date)
        ),
        (
            pl.col(col_to_repeat)
            .filter(pl.col(col_to_repeat).is_not_null())
            .last()
            .over(
                partition_by=IndCQC.location_id,
                order_by=IndCQC.cqc_location_import_date,
            )
            .alias(last_known_value)
        ),
    )

    return lf


def forward_fill(
    lf_with_last_known: pl.LazyFrame,
    col_to_repeat: str,
    days_to_repeat: int,
    last_known_date_col: str = "last_known_date",
    last_known_value_col: str = "last_known_value",
) -> pl.LazyFrame:
    """
    Forward fill column to repeat based on certain conditions
    
    This function forward fills the column to repeat with the last known value for the number of days we want it
    to repeat. It checks if the import date and last known date is less than days_to_repeat, if it is it fills
    those with last known value calculated in previous function.

    Args:
        lf_with_last_known (pl.LazyFrame): Input polars LazyFrame with last know value and date.
        col_to_repeat (str): Column name which needs forward filling.
        days_to_repeat (int): Number of days a value needs to be repeated.
        last_known_date_col (str): Last Known Date column name defaulted to last_known_date.
        last_known_value_col (str): Last Known Value column defaulted to last_known_value

    Returns:
        pl.LazyFrame: A polars LazyFrame with forward filled column.
    """
    forward_fill_condition = pl.col(col_to_repeat).is_null()
    after_last_known_date_condition = pl.col(IndCQC.cqc_location_import_date) > pl.col(
        last_known_date_col
    )
    within_days_to_fill_condition = (
        pl.col(IndCQC.cqc_location_import_date) - pl.col(last_known_date_col)
    ).dt.total_days() <= days_to_repeat

    lf_with_forward_fill = lf_with_last_known.with_columns(
        pl.when(
            forward_fill_condition
            & after_last_known_date_condition
            & within_days_to_fill_condition
        )
        .then(pl.col(last_known_value_col))
        .otherwise(pl.col(col_to_repeat))
        .alias(col_to_repeat),
    )

    return lf_with_forward_fill.drop([last_known_date_col, last_known_value_col])
