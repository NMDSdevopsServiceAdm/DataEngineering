import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def impute_missing_struct_columns(
    lf: pl.LazyFrame, column_names: list[str]
) -> pl.LazyFrame:
    """
    Imputes missing (None or empty struct) entries in multiple struct columns in a LazyFrame.

    Steps:
    1. Sort the LazyFrame by date for each location ID.
    2. For each column,
        - Creates a new column with the prefix 'imputed_
        - Copies over non-missing struct values (treating empty structs as null)
        - Forward-fills (populate with last known value) missing entries.
        - Backward-fills (next known) missing entries.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the struct column.
        column_names (list[str]): Names of struct columns to impute.

    Returns:
        pl.LazyFrame: LazyFrame with a new set of columns called `imputed_<column_name>` containing imputed values.
    """
    for column_name in column_names:
        new_col = f"imputed_{column_name}"

        lf = lf.with_columns(
            pl.when((pl.col(column_name).list.len().fill_null(0) > 0))
            .then(pl.col(column_name))
            .otherwise(None)
            .alias(new_col)
        )

        lf = lf.with_columns(
            pl.col(new_col)
            .forward_fill()
            .backward_fill()
            .over(partition_by=CQCLClean.location_id, order_by=CQCLClean.import_date)
            .alias(new_col)
        )

    return lf
