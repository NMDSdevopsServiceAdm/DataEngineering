import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    RelatedLocation,
    Sector,
    Services,
)


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

    If a col's type in cols_to_impute is a list, then empty lists in that col are replaced with null before
    values are filled forwards and backwards.

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with columns to impute
        cols_to_impute (list[str]): List of column names to impute missing values for

    Returns:
        pl.LazyFrame: Dataframe with imputed columns
    """
    schema = cqc_lf.collect_schema()
    for col in schema:
        if isinstance(schema[col], pl.datatypes.List):
            cqc_lf = cqc_lf.with_columns(
                pl.when(pl.col(col).list.len() == 0)
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )

    for col in cols_to_impute:
        cqc_lf = cqc_lf.with_columns(
            pl.col(col)
            .forward_fill()
            .backward_fill()
            .over(
                partition_by=CQCLClean.location_id,
                order_by=CQCLClean.cqc_location_import_date,
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


def allocate_primary_service_type(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Allocates the primary service type for each row in the LazyFrame based on the services_offered column.

    primary_service_type is allocated in the following order:
    1) Firstly identify all locations who offer "Care home service with nursing"
    2) Of those who don't, identify all locations who offer "Care home service without nursing"
    3) All other locations are identified as being non residential

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the 'services_offered' column.

    Returns:
        pl.LazyFrame: The LazyFrame with the new 'primary_service_type' column added.
    """
    lf = lf.with_columns(
        pl.when(
            pl.col(CQCLClean.services_offered).list.contains(
                Services.care_home_service_with_nursing
            )
        )
        .then(pl.lit(PrimaryServiceType.care_home_with_nursing))
        .when(
            pl.col(CQCLClean.services_offered).list.contains(
                Services.care_home_service_without_nursing
            )
        )
        .then(pl.lit(PrimaryServiceType.care_home_only))
        .otherwise(pl.lit(PrimaryServiceType.non_residential))
        .alias(CQCLClean.primary_service_type)
    )

    return lf


def realign_carehome_column_with_primary_service(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Allocates the location as a care_home if primary_service_type is a care home.

    Following some missing values in CQC data, the services were imputed. The care_home column is automatically given
    'N' if service is missing. Therefore if imputed service values implied the location was a care home then the
    care_home column needs assigning 'Y'.

    Args:
        lf (pl.LazyFrame): The input LazyFrame containing the 'primary_service_type' column.

    Returns:
        pl.LazyFrame: The LazyFrame with the 'care_home' column realigned with the 'primary_service_type' column.
    """
    lf = lf.with_columns(
        pl.when(
            pl.col(CQCLClean.primary_service_type).is_in(
                [
                    PrimaryServiceType.care_home_with_nursing,
                    PrimaryServiceType.care_home_only,
                ]
            )
        )
        .then(pl.lit(CareHome.care_home))
        .otherwise(pl.lit(CareHome.not_care_home))
        .alias(CQCLClean.care_home)
    )

    return lf


def add_related_location_column(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column which flags whether the location was related to a previous location or not.

    If the columns relationships_types contains "HSCA Predecessor" then "Y", otherwise "N".

    Args:
        cqc_lf (pl.LazyFrame): A LazyFrame with the relationships_types column.

    Returns:
        pl.LazyFrame: LazyFrame with an added related_location column.
    """
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.relationships_types).list.contains("HSCA Predecessor"))
        .then(pl.lit(RelatedLocation.has_related_location))
        .otherwise(pl.lit(RelatedLocation.no_related_location))
        .alias(CQCLClean.related_location)
    )

    return cqc_lf


def clean_and_impute_registration_date(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a new column imputed_registration_date.

    1. Copy registration_date into imputed_registration_date when it is prior to earliest cqc_location_import_date, otherwise null.
    2. Fill nulls in imputed_registration_date based on:
        - when locationid has a registration_date at any point in time, then fill with min registration_date.
        - when locationid has no registration_date at any point in time, then fill with min cqc_location_import_date.

    Args:
        cqc_lf (pl.LazyFrame): A LazyFrame with columns registration_date and cqc_location_import_date.

    Returns:
        pl.LazyFrame: Input LazyFrame with new column imputed_registration_date.

    """

    # 1. Copy registration_date into imputed_registration_date
    cqc_lf = cqc_lf.with_columns(
        pl.when(
            pl.col(CQCLClean.registration_date)
            <= pl.col(CQCLClean.cqc_location_import_date)
            .min()
            .over(CQCLClean.location_id)
        )
        .then(pl.col(CQCLClean.registration_date))
        .otherwise(None)
        .alias(CQCLClean.imputed_registration_date)
    )

    # 2. Fill nulls in imputed_registration_date
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.imputed_registration_date).is_null())
        .then(
            pl.col(CQCLClean.imputed_registration_date)
            .min()
            .over(CQCLClean.location_id)
            .fill_null(
                pl.col(CQCLClean.cqc_location_import_date)
                .min()
                .over(CQCLClean.location_id)
            )
        )
        .otherwise(pl.col(CQCLClean.imputed_registration_date))
        .alias(CQCLClean.imputed_registration_date)
    )

    return cqc_lf


def remove_specialist_colleges(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Removes rows where 'Specialist college service' is the only service in 'services_offered' list column.

    We do not include locations which are only specialist colleges in our
    estimates. This function identifies and removes the ones listed in the locations dataset.

    Args:
        cqc_lf (pl.LazyFrame): A LazyFrame with column services_offered.

    Returns:
        pl.LazyFrame: The imput LazyFrame with locations which are only specialist colleges removed.
    """

    cqc_lf = cqc_lf.filter(
        (pl.col(CQCLClean.services_offered).list.len() != 1)
        | (
            pl.col(CQCLClean.services_offered).list.first()
            != Services.specialist_college_service
        )
        | pl.col(CQCLClean.services_offered).is_null()
    )

    return cqc_lf
