import polars as pl
from click.decorators import pass_meta_key

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import (
    LocationType,
    RegistrationStatus,
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


def impute_historic_relationships(
    cqc_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Imputes historic relationships for locations in the given DataFrame.

    1. Get the first non-null relationship for each location id: first_known_relationships
    2. Get first known relationship which is 'HSCA Predecessor': relationships_predecessors_only
    3. Impute relationships such that:
       - If 'relationships' is not null, use 'relationships'.
       - If 'registration_status' is 'deregistered', use 'first_known_relationships'.
       - If 'registration_status' is 'registered', use 'relationships_predecessors_only'.
    4. Drop intermediate columns

    Args:
        cqc_df (pl.DataFrame): Dataframe with relationship columns

    Returns:
        pl.DataFrame: Dataframe with imputed relationship columns
    """
    # 1. Get the first non-null relationship for each location id: first_known_relationships
    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.relationships)
        .first()
        .over(
            partition_by=CQCLClean.location_id,
            order_by=CQCLClean.cqc_location_import_date,
        )
        .alias(CQCLClean.first_known_relationships),
    )

    # 2. Get first known relationship which is 'HSCA Predecessor': relationships_predecessors_only
    cqc_df = get_predecessor_relationships(cqc_df)

    # 3. Impute relationships
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(CQCLClean.relationships).is_not_null())
        .then(pl.col(CQCLClean.relationships))
        .when(pl.col(CQCLClean.registration_status) == RegistrationStatus.deregistered)
        .then(pl.col(CQCLClean.first_known_relationships))
        .when(pl.col(CQCLClean.registration_status) == RegistrationStatus.registered)
        .then(pl.col(CQCLClean.relationships_predecessors_only))
        .alias(CQCLClean.imputed_relationships)
    )

    # 4. Drop intermediate columns
    cqc_df = cqc_df.drop(
        CQCLClean.first_known_relationships, CQCLClean.relationships_predecessors_only
    )

    return cqc_df


def get_predecessor_relationships(
    cqc_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Filters and aggregates relationships of type 'HSCA Predecessor' for each location.

    1. For each location id flatten first known relationship column.
    2. Filter flattened relationships for type 'HSCA Predecessor'
    3. Recollate flattened relationships to single row per location id.
    4. Join to input

    Args:
        cqc_df (pl.DataFrame): Dataframe with first known relationship column

    Returns:
        pl.DataFrame: Dataframe with additional predecessor relationship column

    """
    # 1. For each location id flatten first known relationship column.
    location_id_map = cqc_df.select(
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
    cqc_df = cqc_df.join(predecessor_agg, on=CQCLClean.location_id, how="left")

    return cqc_df
