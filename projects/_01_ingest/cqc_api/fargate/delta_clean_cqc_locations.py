import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


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
    # 1. replacing provider ids with more than 14 characters with Null
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).str.len_chars() <= 14)
        .then(pl.col(CQCLClean.provider_id))
        .otherwise(None)
        .alias(CQCLClean.provider_id)
    )

    # 2. forward and backwards filling missing provider ids over location id
    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.provider_id)
        .forward_fill()
        .backward_fill()
        .over(CQCLClean.location_id)
    )
    return cqc_df


def clean_and_impute_registration_date(
    cqc_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Cleans and imputes registration date by:
    1. Copying all existing registration dates into the imputed column
    2. Removing any time elements from the (imputed) registration date
    3. Replacing registration dates that are after the import date with null
    4. Replacing registration dates with the minimum registration date for that location id
    5. Replacing registration dates with the minimum import date if there are no registration dates for that location

    Args:
        cqc_df (pl.DataFrame): Dataframe with registration and import date columns

    Returns:
        pl.DataFrame: Dataframe with imputed registration date column

    """
    # 1. Copying all existing registration dates into the imputed column
    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.registration_date).alias(CQCLClean.imputed_registration_date)
    )

    # 2. Removing any time elements from the (imputed) registration date
    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.imputed_registration_date).str.slice(0, 10)
    )

    # 3. Replacing registration dates that are after the import date with null
    cqc_df = cqc_df.with_columns(
        pl.when(
            pl.col(CQCLClean.imputed_registration_date).str.to_date(format="%Y-%m-%d")
            <= pl.col(Keys.import_date).str.to_date(format="%Y%m%d")
        )
        .then(pl.col(CQCLClean.imputed_registration_date))
        .otherwise(None)
    )

    # 4. Replacing registration dates with the minimum registration date for that location id
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(CQCLClean.imputed_registration_date).is_null())
        .then(
            pl.col(CQCLClean.imputed_registration_date)
            .min()
            .over(CQCLClean.location_id)
        )
        .otherwise(pl.col(CQCLClean.imputed_registration_date))
        .alias(CQCLClean.imputed_registration_date)
    )

    # 5. Replacing registration dates with the minimum import date if there are no registration dates for that location
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(CQCLClean.imputed_registration_date).is_null())
        .then(
            pl.col(Keys.import_date)
            .min()
            .over(CQCLClean.location_id)
            .str.strptime(pl.Date, format="%Y%m%d")
            .dt.strftime("%Y-%m-%d")
        )
        .otherwise(pl.col(CQCLClean.imputed_registration_date))
        .alias(CQCLClean.imputed_registration_date)
    )

    return cqc_df
