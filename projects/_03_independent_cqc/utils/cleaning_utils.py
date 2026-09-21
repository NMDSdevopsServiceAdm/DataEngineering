import polars as pl

from polars_utils.cleaning_utils import remove_repeated_values_over_time

_COMPOSITE_DEDUP_STRUCT_COLUMN = "_composite_dedup_struct"
_PERCENTAGE_SHARE_SUM_COLUMN = "_percentage_share_sum"


def remove_repeated_values_over_time_as_group(
    lf: pl.LazyFrame,
    columns_to_clean: list[str],
    partition_by_columns: str | list[str],
    date_column: str,
) -> pl.LazyFrame:
    """
    Replaces consecutive repeated values with null across a group of columns as a
    single unit.

    Unlike `polars_utils.cleaning_utils.remove_repeated_values_over_time`, which
    dedups each column independently, this treats `columns_to_clean` as one
    composite value: a row is only a repeat of the prior row in its partition if
    ALL of `columns_to_clean` are unchanged. If any one of them changes, none of
    them are nulled.

    Packs `columns_to_clean` into a single struct column and delegates to
    `remove_repeated_values_over_time`, relying on struct equality being
    field-wise and null-safe - two consecutive rows where every field is null
    compare as unchanged, rather than propagating null/unknown the way a plain
    nullable-column comparison would.

    Args:
        lf (pl.LazyFrame): The LazyFrame to clean.
        columns_to_clean (list[str]): Column names to dedup as a single unit.
        partition_by_columns (str | list[str]): Column(s) identifying each
            entity's timeline.
        date_column (str): Column to order rows by within each partition.

    Returns:
        pl.LazyFrame: The input LazyFrame with one new "<original>_dedup" column
            per input column.
    """
    lf = lf.with_columns(
        pl.struct(columns_to_clean).alias(_COMPOSITE_DEDUP_STRUCT_COLUMN)
    )

    lf = remove_repeated_values_over_time(
        lf,
        columns_to_clean=[_COMPOSITE_DEDUP_STRUCT_COLUMN],
        partition_by_columns=partition_by_columns,
        date_column=date_column,
    )

    composite_dedup_column = f"{_COMPOSITE_DEDUP_STRUCT_COLUMN}_deduplicated"

    lf = lf.with_columns(
        [
            pl.col(composite_dedup_column).struct.field(column).alias(f"{column}_dedup")
            for column in columns_to_clean
        ]
    ).drop(_COMPOSITE_DEDUP_STRUCT_COLUMN, composite_dedup_column)

    return lf


def percentage_share_horizontal(
    lf: pl.LazyFrame,
    columns: list[str],
    output_columns: list[str],
) -> pl.LazyFrame:
    """
    Adds a row-wise percentage-share column per input column, across `columns`.

    `columns[i]`'s share is written to `output_columns[i]`. The denominator is the
    row-wise sum of `columns`. A row's outputs are all null when the sum is null
    or zero, or when ANY of `columns` is null for that row - a strict guard,
    since `pl.sum_horizontal` treats null inputs as 0, so a partial-null row
    (e.g. [null, 2, 2]) would otherwise silently produce a real, nonzero
    denominator and non-null shares for the populated columns.

    This is intentionally defensive even though no current caller can produce a
    partial-null row (their upstream data is confirmed all-null-or-all-populated
    as a group) - written for reuse by other row-wise breakdowns (e.g. gender,
    ethnicity) that may not share that guarantee.

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
    lf = lf.with_columns(pl.sum_horizontal(columns).alias(_PERCENTAGE_SHARE_SUM_COLUMN))

    any_column_is_null = pl.any_horizontal([pl.col(c).is_null() for c in columns])
    has_valid_denominator = (
        pl.col(_PERCENTAGE_SHARE_SUM_COLUMN).is_not_null()
        & (pl.col(_PERCENTAGE_SHARE_SUM_COLUMN) != 0)
        & ~any_column_is_null
    )

    percentage_exprs = [
        pl.when(has_valid_denominator)
        .then(
            pl.col(column).cast(pl.Float32)
            / pl.col(_PERCENTAGE_SHARE_SUM_COLUMN).cast(pl.Float32)
        )
        .otherwise(pl.lit(None).cast(pl.Float32))
        .alias(output_column)
        for column, output_column in zip(columns, output_columns)
    ]

    lf = lf.with_columns(percentage_exprs).drop(_PERCENTAGE_SHARE_SUM_COLUMN)

    return lf
