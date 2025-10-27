import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import Sector


def clean_provider_id_column(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Replaces `provider_id` strings with more than 14 characters with null.

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with `provider_id` column

    Returns:
        pl.LazyFrame: Dataframe with cleaned `provider_id` column
    """
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).str.len_chars() <= 14)
        .then(pl.col(CQCLClean.provider_id))
        .otherwise(None)
        .alias(CQCLClean.provider_id)
    )
    return cqc_lf


def impute_missing_values(
    cqc_lf: pl.LazyFrame, cols_to_impute: list[str]
) -> pl.LazyFrame:
    """
    Imputes missing values in specified columns by forward and backwards filling over location ID.

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with columns to impute
        cols_to_impute (list[str]): List of column names to impute missing values for

    Returns:
        pl.LazyFrame: Dataframe with imputed columns
    """
    for col in cols_to_impute:
        cqc_lf = cqc_lf.with_columns(
            pl.col(col)
            .forward_fill()
            .backward_fill()
            .over(
                partition_by=CQCLClean.location_id,
                order_by=CQCLClean.import_date,
            )
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
