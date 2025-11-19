import polars as pl

from polars_utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import CareHome, PrimaryServiceType
from utils.column_values.categorical_column_values import (
    PrimaryServiceTypeSecondLevel as PSSL_values,
)
from utils.column_values.categorical_column_values import (
    RelatedLocation,
    Sector,
    Services,
)
from utils.column_values.categorical_column_values import (
    SpecialistGeneralistOther as SpecGenOther,
)
from utils.value_labels.ind_cqc_filled_posts.primary_service_type_mapping import (
    CqcServiceToPrimaryServiceTypeSecondLevelLookup as PSSL_lookup,
)


def save_latest_full_snapshot(
    cqc_lf: pl.LazyFrame, cqc_full_snapshot_destination: str
) -> None:
    """
    Saves selected columns for the most recent import date as a parquet file.

    All registered and deregistered locations in the latest full snapshot of CQC locations are saved.
    This data is used in the Skills for Care (SfC) internal reconciliation process.

    Args:
        cqc_lf (pl.LazyFrame): LazyFrame containing CQC location data.
        cqc_full_snapshot_destination (str): S3 URI to save CQC deregistered locations data.
    """
    required_columns = [
        CQCLClean.cqc_location_import_date,
        CQCLClean.location_id,
        CQCLClean.registration_status,
        CQCLClean.deregistration_date,
        CQCLClean.type,
    ]

    cqc_lf = cqc_lf.select(*required_columns)

    cqc_lf = utils.filter_to_maximum_value_in_column(
        cqc_lf, CQCLClean.cqc_location_import_date
    )

    utils.sink_to_parquet(cqc_lf, cqc_full_snapshot_destination, append=False)


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


def classify_specialisms(
    cqc_lf: pl.LazyFrame, list_of_specialisms: list[str]
) -> pl.LazyFrame:
    """
    Adds a new column per element in given list_of_specialisms to show if the location is
    a 'specialist', 'generalist' or 'other' in that specialism.

    A specialist is a location that only offers the given specialism.
    A generalist is a location that offers the given specialism amongst other specialisms.
    Other is location that does not offer the given specialism.

    Args:
        cqc_lf (pl.LazyFrame): A LazyFrame with the column specialisms_offered.
        list_of_specialisms (list[str]): A list of values from specialisms_offered to classify locations by.

    Returns:
        pl.LazyFrame: The input LazyFrame with additional columns per element in given list_of_specialisms.
    """
    specialisms_col = pl.col(CQCLClean.specialisms_offered)

    cqc_lf = cqc_lf.with_columns(
        [
            (
                pl.when(
                    (specialisms_col.list.contains(s))
                    & (specialisms_col.list.len() == 1)
                )
                .then(pl.lit(SpecGenOther.specialist))
                .when(specialisms_col.list.contains(s))
                .then(pl.lit(SpecGenOther.generalist))
                .otherwise(pl.lit(SpecGenOther.other))
                .alias(f"specialism_{s}".replace(" ", "_").lower())
            )
            for s in list_of_specialisms
        ]
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

    specialist_college_only = (pl.col(CQCLClean.services_offered).list.len() == 1) & (
        pl.col(CQCLClean.services_offered).list.first()
        == Services.specialist_college_service
    )

    cqc_lf = cqc_lf.remove(specialist_college_only)

    return cqc_lf


def allocate_primary_service_type_second_level(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column called primary_service_type_second_level which shows the allocated service type per location.

    The function builds an expression of nested when clauses using the order of the keys in the
    PSSL_lookup dict. For example, when 'services_offered' column contains 'key 1' then 'Value 1',
    otherwise when 'services_offered' contains 'key 2' then 'value 2', otherwise...

    Therefore, the lookup_dict order determines the priority of which primary_service_type_second_level
    is allocated to a location if they have multiple 'services_offered'.
    When none of the `services_offered` are in the lookup_dict keys, then the row
    gets the default value 'Other non-residential'.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing the 'services_offered' column.

    Returns:
        pl.LazyFrame: The input LazyFrame with a new column called 'primary_service_type_second_level'.
    """

    lookup_dict = list(PSSL_lookup.dict.items())

    condition = pl.lit(PSSL_values.other_non_residential)
    for description, primary_service_type_second_level in lookup_dict:
        condition = (
            pl.when(pl.col(CQCLClean.services_offered).list.contains(description))
            .then(pl.lit(primary_service_type_second_level))
            .otherwise(condition)
        )

    lf = lf.with_columns(condition.alias(CQCLClean.primary_service_type_second_level))

    return lf
