import operator
from functools import reduce

import polars as pl

from polars_utils.cleaning_utils import remove_repeated_values_over_time


def remove_repeated_values_over_time_as_group(
    lf: pl.LazyFrame,
    columns_to_clean: list[str],
    partition_by_columns: str | list[str],
    date_column: str,
    workplace_columns: list[str] | None = None,
    independent: bool = False,
) -> pl.LazyFrame:
    """
    Nulls a value that repeats the prior row in its partition, across one or
    more columns at once, optionally broadcasting the decision across a
    workplace.

    By default, `columns_to_clean` is treated as one composite unit via a
    null-safe struct comparison: any one column changing keeps all of them. With
    `independent=True`, each column is instead judged on its own, but all
    columns are still computed in one batched set of `.over()` passes rather
    than one per column.

    If `workplace_columns` is given, a row is only nulled when no row sharing
    those values changed - broadcast with `.over()` rather than a group_by +
    join, which costs more peak memory. Rows with a null workplace column are
    judged on their own row only. With `independent=True`, this broadcast runs
    separately per column.

    Args:
        lf (pl.LazyFrame): The LazyFrame to clean.
        columns_to_clean (list[str]): Columns to dedup, as one unit unless
            `independent` is True.
        partition_by_columns (str | list[str]): Column(s) identifying each
            entity's timeline.
        date_column (str): Column to order rows by within each partition.
        workplace_columns (list[str] | None): Columns identifying a workplace on
            a date. Defaults to None, which judges each timeline alone.
        independent (bool): Judge each column on its own rather than as one
            composite unit. Defaults to False.

    Returns:
        pl.LazyFrame: The input LazyFrame with one new "<original>_dedup" column
            per input column.
    """
    if independent:
        partition_columns = (
            [partition_by_columns]
            if isinstance(partition_by_columns, str)
            else partition_by_columns
        )
        changed_columns = {
            column: f"_row_changed_{column}" for column in columns_to_clean
        }

        changed_exprs = []
        for column in columns_to_clean:
            last_value = (
                pl.col(column)
                .shift(1)
                .over(
                    partition_by=partition_columns,
                    order_by=[*partition_columns, date_column],
                )
            )
            changed_exprs.append(
                (last_value.is_null() | (pl.col(column) != last_value)).alias(
                    changed_columns[column]
                )
            )
        lf = lf.with_columns(changed_exprs)

        if workplace_columns is not None:
            workplace_is_known = pl.all_horizontal(
                [pl.col(c).is_not_null() for c in workplace_columns]
            )
            lf = lf.with_columns(
                [
                    pl.when(workplace_is_known)
                    .then(pl.col(changed_columns[column]).any().over(workplace_columns))
                    .otherwise(pl.col(changed_columns[column]))
                    .alias(changed_columns[column])
                    for column in columns_to_clean
                ]
            )

        lf = lf.with_columns(
            [
                pl.when(pl.col(changed_columns[column]))
                .then(pl.col(column))
                .otherwise(None)
                .alias(f"{column}_dedup")
                for column in columns_to_clean
            ]
        )
        return lf.drop(list(changed_columns.values()))

    if workplace_columns is not None:
        partition_columns = (
            [partition_by_columns]
            if isinstance(partition_by_columns, str)
            else partition_by_columns
        )
        row_struct = pl.struct(columns_to_clean)
        last_struct = row_struct.shift(1).over(
            partition_by=partition_columns,
            order_by=[*partition_columns, date_column],
        )
        row_changed = last_struct.is_null() | (row_struct != last_struct)
        workplace_is_known = pl.all_horizontal(
            [pl.col(c).is_not_null() for c in workplace_columns]
        )
        changed_column = "_row_changed"
        keep_column = "_keep_row"

        # Two steps: nesting the workplace `.over()` around the per-timeline
        # `.over()` gives wrong results, so the per-row flag is materialised first.
        lf = lf.with_columns(row_changed.alias(changed_column))
        lf = lf.with_columns(
            pl.when(workplace_is_known)
            .then(pl.col(changed_column).any().over(workplace_columns))
            .otherwise(pl.col(changed_column))
            .alias(keep_column)
        )
        lf = lf.with_columns(
            [
                pl.when(pl.col(keep_column))
                .then(pl.col(column))
                .otherwise(None)
                .alias(f"{column}_dedup")
                for column in columns_to_clean
            ]
        )
        return lf.drop(changed_column, keep_column)

    composite_dedup_struct_column = "_composite_dedup_struct"

    lf = lf.with_columns(
        pl.struct(columns_to_clean).alias(composite_dedup_struct_column)
    )

    lf = remove_repeated_values_over_time(
        lf,
        columns_to_clean=[composite_dedup_struct_column],
        partition_by_columns=partition_by_columns,
        date_column=date_column,
    )

    composite_dedup_column = f"{composite_dedup_struct_column}_deduplicated"

    lf = lf.with_columns(
        [
            pl.col(composite_dedup_column).struct.field(column).alias(f"{column}_dedup")
            for column in columns_to_clean
        ]
    ).drop(composite_dedup_struct_column, composite_dedup_column)

    return lf


def percentage_share_horizontal(
    lf: pl.LazyFrame,
    columns: list[str],
    output_columns: list[str],
) -> pl.LazyFrame:
    """
    Adds a row-wise percentage-share column per input column.

    `columns[i]`'s share of the row-wise sum of `columns` is written to
    `output_columns[i]`. A row's outputs are all null when the sum is null or
    zero, or when ANY of `columns` is null (`pl.sum_horizontal` treats nulls as
    0, so a partial-null row would otherwise get non-null shares). The guard is
    deliberately strict for reuse by other breakdowns (e.g. gender, ethnicity).

    Args:
        lf (pl.LazyFrame): The LazyFrame to add percentage columns to.
        columns (list[str]): Column names to compute a horizontal percentage
            share across.
        output_columns (list[str]): Output column names, paired positionally
            with `columns`.

    Returns:
        pl.LazyFrame: The input LazyFrame with one new Float32 percentage column
            per pair in `columns`/`output_columns`.
    """
    percentage_share_sum_column = "_percentage_share_sum"

    lf = lf.with_columns(pl.sum_horizontal(columns).alias(percentage_share_sum_column))

    any_column_is_null = pl.any_horizontal([pl.col(c).is_null() for c in columns])
    has_valid_denominator = (
        pl.col(percentage_share_sum_column).is_not_null()
        & (pl.col(percentage_share_sum_column) != 0)
        & ~any_column_is_null
    )

    percentage_exprs = [
        pl.when(has_valid_denominator)
        .then(
            pl.col(column).cast(pl.Float32)
            / pl.col(percentage_share_sum_column).cast(pl.Float32)
        )
        .otherwise(pl.lit(None).cast(pl.Float32))
        .alias(output_column)
        for column, output_column in zip(columns, output_columns)
    ]

    lf = lf.with_columns(percentage_exprs).drop(percentage_share_sum_column)

    return lf


def null_columns_where_group_share_too_low(
    lf: pl.LazyFrame,
    partition_by_columns: list[str],
    total_columns: list[str],
    share_columns: list[str],
    columns_to_null: list[str],
    maximum_share: float,
) -> pl.LazyFrame:
    """
    Nulls `columns_to_null` for every row in a group where `share_columns` make
    up at most `maximum_share` of the group's total.

    Per group, sums `total_columns` across all rows for the total and
    `share_columns` for the numerator. The decision is broadcast to every row of
    the group with `.over()` rather than a group_by + join, which costs more
    peak memory.

    A row's total is null if any of `total_columns` is null, so such rows drop
    out of the group sums instead of counting as zero. Rows with a null
    partition column are never flagged, so unrelated null-keyed rows aren't
    pooled.

    Args:
        lf (pl.LazyFrame): The LazyFrame to clean.
        partition_by_columns (list[str]): Columns defining a group.
        total_columns (list[str]): Columns summed for the group total. Must
            include `share_columns`.
        share_columns (list[str]): Columns summed for the numerator.
        columns_to_null (list[str]): Columns to null for flagged groups.
        maximum_share (float): Largest numerator / total share that is nulled.

    Returns:
        pl.LazyFrame: The input LazyFrame with `columns_to_null` nulled for
            flagged groups.

    Raises:
        ValueError: If `share_columns` isn't a subset of `total_columns`.
    """
    if not set(share_columns) <= set(total_columns):
        raise ValueError("share_columns must be a subset of total_columns")

    flag_column = "_share_too_low"

    group_total = reduce(operator.add, map(pl.col, total_columns)).sum()
    group_share_total = reduce(operator.add, map(pl.col, share_columns)).sum()
    partition_is_not_null = pl.all_horizontal(
        [pl.col(c).is_not_null() for c in partition_by_columns]
    )

    share_too_low = partition_is_not_null & (
        group_share_total.over(partition_by_columns)
        / group_total.over(partition_by_columns)
        <= maximum_share
    )

    # Computed once into a flag column, so each `when` below reads it rather than
    # re-evaluating the window expressions for every column.
    lf = lf.with_columns(share_too_low.alias(flag_column))
    lf = lf.with_columns(
        [
            pl.when(pl.col(flag_column)).then(None).otherwise(pl.col(c)).alias(c)
            for c in columns_to_null
        ]
    )
    return lf.drop(flag_column)
