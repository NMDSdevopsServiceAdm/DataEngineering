import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import Sector


def clean_provider_id_column(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Cleans provider ID column, removing long IDs and then forwards and backwards filling the value

     1. Replace provider ids with more than 14 characters with Null
     2. Forward and backwards fill missing provider ids over location id
    Args:
        cqc_lf (pl.LazyFrame): Dataframe with provider id column

    Returns:
        pl.LazyFrame: Dataframe with cleaned provider id column

    """
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).str.len_chars() <= 14)
        .then(pl.col(CQCLClean.provider_id))
        .otherwise(None)
        .alias(CQCLClean.provider_id)
    )

    cqc_lf = cqc_lf.with_columns(
        pl.col(CQCLClean.provider_id)
        .forward_fill()
        .backward_fill()
        .over(CQCLClean.location_id)
    )
    return cqc_lf


def assign_cqc_sector(cqc_lf: pl.LazyFrame, la_provider_ids: list[str]) -> pl.LazyFrame:
    """
    Assign CQC sector for each row based on the Provider ID.

    1. If the Provider ID is in the list of la_provider_ids then assign "Local authority"
    2. Otherwise, assign "Independent"

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with provider id column.
        la_provider_ids (list[str]): List of provider IDs that indicate a location is part of the local authority.

    Returns:
        pl.LazyFrame: Input dataframe with new CQC sector column.
    """
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).is_in(la_provider_ids))
        .then(pl.lit(Sector.local_authority))
        .otherwise(pl.lit(Sector.independent))
        .alias(CQCLClean.cqc_sector)
    )
    return cqc_lf


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
    identifier_col = CQCLClean.location_id
    date_col = CQCLClean.import_date

    lf = lf.sort([identifier_col, date_col])

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
            .over(identifier_col)
            .alias(new_col)
        )

    return lf
