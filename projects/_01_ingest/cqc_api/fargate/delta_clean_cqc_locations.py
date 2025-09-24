import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import (
    RegistrationStatus,
    PrimaryServiceType,
    RelatedLocation,
    Services,
    Sector,
)


def clean_provider_id_column(cqc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans provider ID column, removing long IDs and then forwards and backwards filling the value

     1. Replace provider ids with more than 14 characters with Null
     2. Forward and backwards fill missing provider ids over location id
    Args:
        cqc_df (pl.DataFrame): Dataframe with provider id column

    Returns:
        pl.DataFrame: Dataframe with cleaned provider id column

    """
    # 1. Replace provider ids with more than 14 characters with Null
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).str.len_chars() <= 14)
        .then(pl.col(CQCLClean.provider_id))
        .otherwise(None)
        .alias(CQCLClean.provider_id)
    )

    # 2. Forward and backwards fill missing provider ids over location id
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
    Cleans registration dates into YYYY-MM-DD format, removing invalid dates and then imputing missing values.

    1. Copy all existing registration dates into the imputed column
    2. Remove any time elements from the (imputed) registration date
    3. Replace registration dates that are after the import date with null
    4. Replace registration dates with the minimum registration date for that location id
    5. Replace registration dates with the minimum import date if there are no registration dates for that location

    Args:
        cqc_df (pl.DataFrame): Dataframe with registration and import date columns

    Returns:
        pl.DataFrame: Dataframe with imputed registration date column

    """
    # 1. Copy all existing registration dates into the imputed column
    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.registration_date).alias(CQCLClean.imputed_registration_date)
    )

    # 2. Remove any time elements from the (imputed) registration date
    cqc_df = cqc_df.with_columns(
        pl.col(CQCLClean.imputed_registration_date).str.slice(0, 10)
    )

    # 3. Replace registration dates that are after the import date with null
    cqc_df = cqc_df.with_columns(
        pl.when(
            pl.col(CQCLClean.imputed_registration_date).str.to_date(format="%Y-%m-%d")
            <= pl.col(Keys.import_date).str.to_date(format="%Y%m%d")
        )
        .then(pl.col(CQCLClean.imputed_registration_date))
        .otherwise(None)
    )

    # 4. Replace registration dates with the minimum registration date for that location id
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

    # 5. Replace registration dates with the minimum import date if there are no registration dates for that location
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


def impute_missing_values_for_struct_column(
    cqc_df: pl.DataFrame, column_name: str
) -> pl.DataFrame:
    """
    Creates new column called 'imputed_[column_name]' containing imputed values for a struct column.

    1. Uses the value in the existing column for 'imputed_[column_name]' if it is a list which contains values.
    2. First forward and then backwards fill any missing values in 'imputed_[column_name]' for each location id

    Args:
        cqc_df (pl.DataFrame): Dataframe containing 'location_id', 'cqc_location_import_date', and a struct column to impute.
        column_name (str): Name of struct column to impute

    Returns:
        pl.DataFrame: DataFrame with the struct column containing imputed values in 'imputed_[column_name]'.
    """
    # 1. Uses the value in the existing column for 'imputed_[column_name]' if it is a list which contains values.
    imputed_column_name = "imputed_" + column_name
    cqc_df = cqc_df.with_columns(
        pl.when(pl.col(column_name).list.len() > 0)
        .then(pl.col(column_name))
        .otherwise(None)
        .alias(imputed_column_name)
    )

    # 2. First forward and then backwards fill any missing values in 'imputed_[column_name]' for each location id
    cqc_df = cqc_df.with_columns(
        pl.col(imputed_column_name)
        .forward_fill()
        .backward_fill()
        .over(
            partition_by=CQCLClean.location_id,
            order_by=CQCLClean.cqc_location_import_date,
        )
    )

    return cqc_df


def assign_primary_service_type(cqc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Allocates the primary service type for each row in the DataFrame based on the descriptions in the 'imputed_gac_service_types' field.

    1. If any of the imputed GAC service descriptions have "Care home service with nursing" allocate as "Care home with nursing"
    2. If not, if any of the imputed GAC service descriptions have "Care home service without nursing" allocate as "Care home without nursing"
    3. Otherwise, allocate as "non-residential"

    Args:
        cqc_df (pl.DataFrame): The input DataFrame containing the 'imputed_gac_service_types' column.

    Returns:
        pl.DataFrame: Dataframe with the new 'primary_service_type' column.

    """
    # 1. If any of the imputed GAC service descriptions have "Care home service with nursing" allocate as "Care home with nursing"
    cqc_df = cqc_df.with_columns(
        pl.when(
            pl.col(CQCLClean.imputed_gac_service_types)
            .list.eval(
                pl.element()
                .struct.field(CQCLClean.description)
                .eq("Care home service with nursing")
            )
            .list.any()
        )
        .then(pl.lit(PrimaryServiceType.care_home_with_nursing))
        # 2. If not, if any of the imputed GAC service descriptions have "Care home service without nursing" allocate as "Care home without nursing"
        .when(
            pl.col(CQCLClean.imputed_gac_service_types)
            .list.eval(
                pl.element()
                .struct.field(CQCLClean.description)
                .eq("Care home service without nursing")
            )
            .list.any()
        )
        .then(pl.lit(PrimaryServiceType.care_home_only))
        # 3. Otherwise, allocate as "non-residential"
        .otherwise(pl.lit(PrimaryServiceType.non_residential))
        .alias(CQCLClean.primary_service_type)
    )

    return cqc_df


def assign_care_home(cqc_df: pl.DataFrame) -> pl.DataFrame:
    pass


def add_related_location_flag(cqc_df: pl.DataFrame) -> pl.DataFrame:
    """
    Adds a column which flags whether the location was related to a previous location or not

    1. If the length of imputed relationships is more than 0 then flag 'Y'
    2. Otherwise, flag 'N'

    Args:
        cqc_df (pl.DataFrame): A dataframe with the imputed_relationships column.

    Returns:
        pl.DataFrame: Dataframe with an added related_location column.
    """
    cqc_df = cqc_df.with_columns(
        # 1. If the length of imputed relationships is more than 0 then flag 'Y'
        pl.when(pl.col(CQCLClean.imputed_relationships).list.len() > 0)
        .then(pl.lit(RelatedLocation.has_related_location))
        # 2. Otherwise, flag 'N'
        .otherwise(pl.lit(RelatedLocation.no_related_location))
        .alias(CQCLClean.related_location)
    )

    return cqc_df


def remove_specialist_colleges(
    cqc_df: pl.DataFrame, gac_services_dimension: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    We do not include locations which are only specialist colleges in our
    estimates. This function identifies and removes the ones listed in the locations dataset.

    1. Filter for rows in the GAC Service dimension where "Specialist college service" is the only service offered
    2. Remove the identified rows from the cqc fact table, and the GAC Service Dimension

    Args:
        cqc_df (DataFrame): Fact table to align with dimension
        gac_services_dimension (DataFrame): Dimension table with services_offered column

    Returns:
        tuple[pl.DataFrame, pl.DataFrame]: cqq_df, gac_services_dimension with locations which are only specialist colleges removed.

    """
    to_remove_df = gac_services_dimension.filter(
        pl.col(CQCLClean.services_offered)
        .list.first()
        .eq(Services.specialist_college_service)
        & pl.col(CQCLClean.services_offered).list.len().eq(1)
        & pl.col(CQCLClean.services_offered).is_not_null()
    ).select(CQCLClean.location_id, Keys.import_date)
    cqc_df, gac_services_dimension = remove_rows(
        to_remove_df=to_remove_df, target_dfs=[cqc_df, gac_services_dimension]
    )
    return cqc_df, gac_services_dimension


def remove_rows(
    to_remove_df: pl.DataFrame, target_dfs: list[pl.DataFrame]
) -> list[pl.DataFrame]:
    """
    Remove rows from a fact table and any provided dimension tables
    Args:
        to_remove_df (pl.DataFrame): Dataframe with rows to remove (all columns present will be used as keys for the anti-join).
        target_dfs (list[pl.DataFrame]): Target tables from which to remove the rows.

    Returns:
        list[pl.DataFrame]: List of dataframes in the same order as target_dfs, with the rows removed.
    """
    result_dfs = []
    for target_df in target_dfs:
        result_dfs.append(
            target_df.join(
                to_remove_df,
                on=to_remove_df.columns,
                how="anti",
            )
        )

    return result_dfs


def assign_cqc_sector(cqc_df: pl.DataFrame, la_provider_ids: list[str]) -> pl.DataFrame:
    """
    Assign CQC sector for each row based on the Provider ID.

    1. If the Provider ID is in the list of la_provider_ids then assign "Local authority"
    2. Otherwise, assign "Independent"

    Args:
        cqc_df (pl.DataFrame): Dataframe with provider id column.
        la_provider_ids (list[str]): List of provider IDs that indicate a location is part of the local authority.

    Returns:
        pl.DataFrame: Input dataframe with new CQC sector column.
    """
    cqc_df = cqc_df.with_columns(
        # 1. If the Provider ID is in the list of la_provider_ids then assign "Local authority"
        pl.when(pl.col(CQCLClean.provider_id).is_in(la_provider_ids))
        .then(pl.lit(Sector.local_authority))
        # 2. Otherwise, assign "Independent"
        .otherwise(pl.lit(Sector.independent))
        .alias(CQCLClean.cqc_sector)
    )
    return cqc_df
