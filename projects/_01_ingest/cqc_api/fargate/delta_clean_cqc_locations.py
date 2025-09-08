import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)


def clean_provider_id_column(cqc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans provider id column by:
     1. replacing provider ids with more than 14 characters with Null
     2. forward and backwards filling missing provider ids over location id
    Args:
        cqc_df (pl.DataFrame): Dataframe with provider id column

    Returns:
        pl.DataFrame: Dataframe with cleaned provider id column

    """
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).str.len_chars() <= 14)
        .then(pl.col(CQCLClean.provider_id))
        .otherwise(None)
        .alias(CQCLClean.provider_id)
    )

    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.provider_id)
        .forward_fill()
        .backward_fill()
        .over(CQCLClean.location_id)
    )
    return cqc_df
