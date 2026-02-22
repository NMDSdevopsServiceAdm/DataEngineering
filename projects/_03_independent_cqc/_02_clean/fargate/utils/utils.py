from typing import Optional
import polars as pl
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def create_column_with_repeated_values_removed(
    lf: pl.LazyFrame,
    column_to_clean: str,
    new_column_name: Optional[str] = None,
    column_to_partition_by: str = IndCQC.location_id,
) -> pl.LazyFrame:
    """
    Some data we have (such as ASCWDS) repeats data until it is changed. This
    function creates a new column which converts repeated values to nulls, so we
    only see newly submitted values once. This also happens as a result of
    joining the same datafile multiple times as part of the align dates field.

    For each partition, this function iterates over the LazyFrame in date order
    and compares the current column value to the previously submitted value. If
    the value differs from the previously submitted value then enter that value
    into the new column. Otherwise null the value in the new column as it is a
    previously submitted value which has been repeated.

    Args:
        lf (pl.LazyFrame): The polars LazyFrame to use
        column_to_clean (str): The name of the column to convert
        new_column_name (Optional [str]): If not provided, "_deduplicated"
            will be appended onto the original column name
        column_to_partition_by (str): A column to partition by when
            deduplicating. Defaults to 'locationid'.

    Returns:
        pl.LazyFrame: A polars LazyFrame with an addional column with repeated
            values changed to nulls.
    """
    if new_column_name is None:
        new_column_name = f"{column_to_clean}_deduplicated"

    previous_value = (
        pl.col(column_to_clean)
        .shift(1)
        .over(
            partition_by=column_to_partition_by,
            order_by=IndCQC.cqc_location_import_date,
        )
    )

    return lf.with_columns(
        pl.when(previous_value.is_null() | (pl.col(column_to_clean) != previous_value))
        .then(pl.col(column_to_clean))
        .otherwise(None)
        .alias(new_column_name)
    )
