import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


def aggregate_values_to_provider_level(
    lf: pl.LazyFrame,
    col_to_sum: str,
) -> pl.LazyFrame:
    """
    Adds a new column with the provider-level sum of a given column.
    The new column will be named col_to_sum suffixed with "_provider_sum".

    Args:
        lf (pl.LazyFrame): A LazyFrame with provider_id and
            cqc_location_import_date columns.
        col_to_sum (str): A column of values to sum.

    Returns:
        pl.LazyFrame: The input LazyFrame with a new aggregated column.
    """

    partition_cols = [IndCQC.provider_id, IndCQC.cqc_location_import_date]

    expr = pl.col(col_to_sum)

    return lf.with_columns(
        pl.when(expr.count().over(partition_cols) > 0)
        .then(expr.sum().over(partition_cols))
        .otherwise(None)
        .alias(f"{col_to_sum}_provider_sum")
    )
