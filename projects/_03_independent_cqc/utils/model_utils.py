import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def add_date_index(
    lf: pl.LazyFrame, partition_columns: list[str], date_column: str
) -> pl.LazyFrame:
    """
    Add each row's index among its partition's distinct dates.

    A "dense" rank is used, so repeated dates share an index and no dates are skipped in the
    sequence. For example, if three rows share the same date, they all receive the same index
    value, and the next distinct date receives the next integer, such as 1, 1, 1, 2 (rather than
    1, 1, 1, 4 as standard ranking would give).

    Args:
        lf (pl.LazyFrame): dataset containing the partition and date columns
        partition_columns (list[str]): the columns identifying each timeline
        date_column (str): the date column

    Returns:
        pl.LazyFrame: dataset with "cqc_location_import_date_indexed" added
    """
    return lf.with_columns(
        pl.col(date_column)
        .rank(method="dense")
        .over(partition_columns)
        .alias(IndCQC.cqc_location_import_date_indexed)
    )
