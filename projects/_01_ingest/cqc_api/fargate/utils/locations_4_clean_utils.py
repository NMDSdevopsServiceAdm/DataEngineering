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


def impute_historic_relationships(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Imputes historic relationships for locations in the given LazyFrame.

    If a location is 'Deregistered' it can have both Predecessors (a previous linked location) and
    Successors (the location it was linked to after closing). A 'Registered' location can only have
    Predecessors. As we are backdating data, locations which were Deregistered when the 'relationship'
    data was first added will need any references to Successors removing when they were previously
    Registered. In order to do this, the function performs the following steps:

    1. Adds a column 'first_known_relationships' with the first non-null relationship for each location.
    2. Filters the relationships to include only those of type 'HSCA Predecessor'.
    3. Adds a column 'imputed_relationships' based on the following conditions:
       - If 'relationships' is not null, use 'relationships'.
       - If 'registration_status' is 'deregistered', use 'first_known_relationships'.
       - If 'registration_status' is 'registered', use 'relationships_predecessors_only'.
    4. Drops the intermediate columns 'first_known_relationships' and 'relationships_predecessors_only'.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing location data and relationships.

    Returns:
        pl.LazyFrame: LazyFrame with imputed historic relationships.
    """
    print("Input lf")
    print(lf.collect().glimpse())

    lf = lf.with_columns(
        pl.col(CQCLClean.relationships)
        .drop_nulls()
        .first()
        .over(
            partition_by=CQCLClean.location_id,
            order_by=CQCLClean.cqc_location_import_date,
        )
        .alias(CQCLClean.first_known_relationships)
    )

    print("After creating first_known_relationships")
    print(lf.collect().glimpse())

    # get_relationships_where_type_is_predecessor
    lf = lf.with_columns(
        pl.col(CQCLClean.first_known_relationships)
        .list.eval(
            pl.when(
                pl.element().struct.field(CQCLClean.type) == "HSCA Predecessor"
            ).then(pl.element())
        )
        .alias(CQCLClean.relationships_predecessors_only)
    )

    print("After creating relationships_predecessors_only")
    print(lf.collect().glimpse())

    lf = lf.with_columns(
        pl.when(pl.col(CQCLClean.relationships).is_not_null())
        .then(pl.col(CQCLClean.relationships))
        .when(pl.col(CQCLClean.registration_status) == RegistrationStatus.deregistered)
        .then(CQCLClean.first_known_relationships)
        .when(pl.col(CQCLClean.registration_status) == RegistrationStatus.registered)
        .then(CQCLClean.relationships_predecessors_only)
        .alias(CQCLClean.imputed_relationships)
    )

    print("After creating imputed_relationships")
    print(lf.collect().glimpse())

    lf = lf.drop(
        CQCLClean.first_known_relationships,
        CQCLClean.relationships_predecessors_only,
    )

    return lf


# def get_relationships_where_type_is_predecessor(lf: pl.LazyFrame) -> pl.LazyFrame:
#     """
#     Filters and aggregates relationships of type 'HSCA Predecessor' for each location.

#     This function performs the following steps:
#     1. Selects distinct location_id and first_known_relationships columns.
#     2. Explodes the first_known_relationships column to create a row for each relationship.
#     3. Filters the exploded relationships to include only those of type 'HSCA Predecessor'.
#     4. Groups by location_id and collects the set of 'HSCA Predecessor' relationships.
#     5. Joins the original LazyFrame with the aggregated LazyFrame on location_id.

#     Args:
#         lf (pl.LazyFrame): Input LazyFrame containing location data and relationships.

#     Returns:
#         pl.LazyFrame: LazyFrame with an additional column for 'HSCA Predecessor' relationships.
#     """
#     distinct_lf = lf.select(
#         CQCLClean.location_id, CQCLClean.first_known_relationships
#     ).unique()

#     # loop through the list of dicts (first known relationships column).
#     # For each element within the list (a dict), go to the key = type and check if it is HSCA Predecessor.
#     # If it is then keep that element of the list.
#     # If it is not then remove that element.
#     # You end up with a list with only type = HSCA Predecessor elements from the original list.

#     # Do I need to do this in a sub function?  Just do it imputation, then you dont need to join.

#     distinct_lf = distinct_lf.with_columns(
#         pl.col(CQCLClean.first_known_relationships)
#         .list.eval(
#             pl.when(
#                 pl.element().struct.field(CQCLClean.type) == "HSCA Predecessor"
#             ).then(pl.element())
#         )
#         .alias(CQCLClean.relationships_predecessors_only)
#     )

#     distinct_lf = distinct_lf.drop(CQCLClean.first_known_relationships)

#     lf = lf.join(distinct_lf, CQCLClean.location_id, "left")

#     return lf
