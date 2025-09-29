import polars as pl


def has_value(df: pl.DataFrame, column: str, partition_by: str) -> pl.Expr:
    """Identifies if a column has a non-null value for each partition (if provided) or the entire
    Series otherwise. For Series containing Lists of values, a non-zero-sized array is treated as
    a valid value.

    Args:
        df (pl.DataFrame): the DataFrame to analyse
        column (str): the column to search in
        partition_by (str): the column to partition by

    Returns:
        pl.Expr: a Polars expression which can be used to construct a DataFrame or result
    """
    dtype = df.get_column(column).dtype

    if dtype.is_numeric():
        # exists a non-zero value within the partition
        return pl.col(column).cast(pl.Boolean).any().over(partition_by)

    if df.schema[column] not in [pl.Array, pl.List]:
        # exists a non-null value within the partition
        return pl.col(column).str.len_chars().cast(pl.Boolean).any().over(partition_by)

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
