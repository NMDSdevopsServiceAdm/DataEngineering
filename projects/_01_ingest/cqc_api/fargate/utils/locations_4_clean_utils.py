import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import RegistrationStatus, Sector


def clean_provider_id_column(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Cleans Provider ID column by removing long IDs then forwards and backwards filling the value.

     1. Replace `provider_id` strings with more than 14 characters with Null
     2. Forward and backwards fill missing `provider_id` over location id
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

    cqc_lf = cqc_lf.with_columns(
        pl.col(CQCLClean.provider_id)
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


def impute_historic_relationships(
    cqc_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Imputes historic relationships for locations in the given LazyFrame.

    1. Get the first non-null relationship for each location id: first_known_relationships
    2. Get first known relationship which is 'HSCA Predecessor': relationships_predecessors_only
    3. Impute relationships such that:
       - If 'relationships' is not null, use 'relationships'.
       - If 'registration_status' is 'deregistered', use 'first_known_relationships'.
       - If 'registration_status' is 'registered', use 'relationships_predecessors_only'.
    4. Drop intermediate columns

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with relationship columns

    Returns:
        pl.LazyFrame: Dataframe with imputed relationship columns
    """
    # 1. Get the first non-null relationship for each location id: first_known_relationships
    cqc_lf = cqc_lf.with_columns(
        pl.col(CQCLClean.relationships)
        .drop_nulls()
        .first()
        .over(
            partition_by=CQCLClean.location_id,
            order_by=CQCLClean.cqc_location_import_date,
        )
        .alias(CQCLClean.first_known_relationships),
    )

    # 2. Get first known relationship which is 'HSCA Predecessor': relationships_predecessors_only
    cqc_lf = get_predecessor_relationships(cqc_lf)

    # 3. Impute relationships
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.relationships).is_not_null())
        .then(pl.col(CQCLClean.relationships))
        .when(pl.col(CQCLClean.registration_status) == RegistrationStatus.deregistered)
        .then(pl.col(CQCLClean.first_known_relationships))
        .when(pl.col(CQCLClean.registration_status) == RegistrationStatus.registered)
        .then(pl.col(CQCLClean.relationships_predecessors_only))
        .alias(CQCLClean.imputed_relationships)
    )

    # 4. Drop intermediate columns
    cqc_lf = cqc_lf.drop(
        CQCLClean.first_known_relationships, CQCLClean.relationships_predecessors_only
    )

    return cqc_lf


def get_predecessor_relationships(
    cqc_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Filters and aggregates relationships of type 'HSCA Predecessor' for each location.

    1. For each location id flatten first known relationship column.
    2. Filter flattened relationships for type 'HSCA Predecessor'
    3. Recollate flattened relationships to single row per location id.
    4. Join to input

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with first known relationship column

    Returns:
        pl.LazyFrame: Dataframe with additional predecessor relationship column

    """
    # 1. For each location id flatten first known relationship column.
    location_id_map = cqc_lf.select(
        CQCLClean.location_id, CQCLClean.first_known_relationships
    ).unique()

    all_relationships = location_id_map.explode([CQCLClean.first_known_relationships])

    # 2. Filter flattened relationships for type 'HSCA Predecessor'
    predecessor_relationships = all_relationships.filter(
        pl.col(CQCLClean.first_known_relationships).struct.field(CQCLClean.type)
        == "HSCA Predecessor"
    ).rename(
        {CQCLClean.first_known_relationships: CQCLClean.relationships_predecessors_only}
    )

    # 3. Recollate flattened relationships to single row per location id.
    predecessor_agg = predecessor_relationships.group_by(CQCLClean.location_id).all()

    # 4. Join to input
    cqc_lf = cqc_lf.join(predecessor_agg, on=CQCLClean.location_id, how="left")

    return cqc_lf
