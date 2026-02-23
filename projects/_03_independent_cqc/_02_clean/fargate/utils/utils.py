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
    Create a new column where consecutive repeated values are replaced with nulls
    within each partition.

    Rows are ordered by import date and each value is compared to the previous
    value in the same partition. If it differs, it is retained in the new column.
    If it is the same as the previous value, it is replaced with null.

    Args:
        lf (pl.LazyFrame): The polars LazyFrame to use
        column_to_clean (str): The name of the column to deduplicate
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
