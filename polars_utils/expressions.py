import polars as pl


def has_value(
    df: pl.DataFrame, column: str, alias: str, partition_by: str | None = None
) -> pl.Expr:
    """Identifies if a column has a non-null value for each partition (if provided) or the entire
    Series otherwise. For Series containing Lists of values, a non-zero-sized array is treated as
    a valid value.

    Args:
        df (pl.DataFrame): the DataFrame to analyse
        column (str): the column to search in
        alias (str): the resulting colum name for the boolean results
        partition_by (str | None, optional): the column to partition by. Defaults to None.

    Returns:
        pl.Expr: a Polars expression which can be used to construct a DataFrame or result
    """
    if df.schema[column] not in [pl.Array, pl.List]:
        # exists a non-null value within the partition
        return pl.col([column]).is_not_null().over(partition_by).alias(alias)

    # exists a non-zero sized array within the partition
    return pl.col([column]).list.len().max().cast(pl.Boolean).over(partition_by)


def str_length_cols(columns: list[str]) -> list[pl.Expr]:
    """Provides the string lengths for each value in a column as a list of column expressions.

    Args:
        columns (list[str]): a list of column(s) to calculate string lengths for

    Returns:
        list[pl.Expr]: a list of Series of string lengths, to be used in constructing a DataFrame
    """
    return [
        pl.col(column).str.len_chars().alias(f"{column}_length") for column in columns
    ]
