import polars as pl


def forward_fill_within_time_limit(
    lf: pl.LazyFrame,
    columns_to_fill: dict[str, str],
    partition_by_columns: list[str],
    date_column: str,
    time_limit: str,
) -> pl.LazyFrame:
    """
    Forward-fills each source column's last known value into later null rows,
    but only within a bounded time window from that last known date.

    For each `source_column`, rows are ordered by `date_column` within each
    `partition_by_columns` group (e.g. a location's timeline for one job
    role). A null is filled with its nearest *preceding* non-null value only
    if it falls on or before `that value's date + time_limit`; nulls outside
    that window, and nulls before any known value in the partition, are left
    null. Known (non-null) values are never overwritten. A partition may
    contain several known values over time (e.g. after deduplication marks
    every genuine change as "known"); each gap is filled from the known value
    immediately before it, not from the partition's overall last known value.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the source columns,
            partition_by_columns and date_column.
        columns_to_fill (dict[str, str]): Maps each source column name to the
            new output column name it should be forward-filled into.
        partition_by_columns (list[str]): Column(s) identifying each entity's
            timeline (e.g. location_id and published_job_role_label).
        date_column (str): Column to determine recency within each partition.
        time_limit (str): Polars offset string (e.g. "6mo") bounding how far
            a known value may be carried forward.

    Returns:
        pl.LazyFrame: The input LazyFrame with one new output column per
            entry in columns_to_fill.
    """
    fill_exprs = []

    for source_column, output_column in columns_to_fill.items():
        is_known = pl.col(source_column).is_not_null()

        last_known_date = (
            pl.when(is_known)
            .then(pl.col(date_column))
            .forward_fill()
            .over(partition_by=partition_by_columns, order_by=date_column)
        )
        last_known_value = (
            pl.when(is_known)
            .then(pl.col(source_column))
            .forward_fill()
            .over(partition_by=partition_by_columns, order_by=date_column)
        )
        within_time_limit = (pl.col(date_column) > last_known_date) & (
            pl.col(date_column) <= last_known_date.dt.offset_by(time_limit)
        )

        fill_exprs.append(
            pl.when(is_known)
            .then(pl.col(source_column))
            .when(within_time_limit)
            .then(last_known_value)
            .otherwise(None)
            .alias(output_column)
        )

    return lf.with_columns(fill_exprs)
