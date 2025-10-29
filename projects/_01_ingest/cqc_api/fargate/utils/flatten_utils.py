import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def flatten_struct_fields(
    lf: pl.LazyFrame, mappings: list[tuple[str, str, str]]
) -> pl.LazyFrame:
    """
    Flattens the `field` from struct column `struct_col` into a new column `new_col` from a mappings list.

    Args:
        lf (pl.LazyFrame): Polars LazyFrame
        mappings (list[tuple[str, str, str]]):
            A list of tuples specifying (struct_col, field, new_col), where:
                - struct_col: Name of the column containing list-of-structs.
                - field: Name of the field to extract from each struct.
                - new_col: Name of the output column to hold the extracted values.

    Returns:
        pl.LazyFrame: Polars LazyFrame with flattened columns
    """
    for struct_col, field, new_col in mappings:
        lf = lf.with_columns(
            (
                pl.col(struct_col)
                .list.eval(pl.element().struct.field(field))
                .alias(new_col)
            )
        )

    return lf
