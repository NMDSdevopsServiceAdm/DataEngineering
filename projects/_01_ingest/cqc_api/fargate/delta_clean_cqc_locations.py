import sys
import warnings

import polars as pl
from polars.exceptions import ColumnNotFoundError, ComputeError
from botocore.exceptions import ClientError

from polars_utils import utils, logger, raw_data_adjustments
from schemas.cqc_locations_schema_polars import POLARS_LOCATION_SCHEMA
from projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names import (
    extract_registered_manager_names,
)
from projects._01_ingest.cqc_api.fargate.utils.postcode_matcher import (
    run_postcode_matching,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    DimensionPartitionKeys as DimensionKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.cqc_local_authority_provider_ids import LocalAuthorityProviderIds
from utils.column_values.categorical_column_values import (
    RegistrationStatus,
    PrimaryServiceType,
    RelatedLocation,
    Services,
    Sector,
    CareHome,
    LocationType,
    SpecialistGeneralistOther,
    Specialisms,
)


cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
dimension_partition_keys = [
    DimensionKeys.year,
    DimensionKeys.month,
    DimensionKeys.day,
    DimensionKeys.last_updated,
    DimensionKeys.import_date,
]

cqc_location_cols_to_import = [
    CQCLClean.location_id,
    CQCLClean.provider_id,
    CQCLClean.name,
    CQCLClean.postal_address_line1,
    CQCLClean.postal_code,
    CQCLClean.registration_status,
    CQCLClean.registration_date,
    CQCLClean.deregistration_date,
    CQCLClean.type,
    CQCLClean.relationships,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.dormancy,
    CQCLClean.gac_service_types,
    CQCLClean.regulated_activities,
    CQCLClean.specialisms,
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
]
ons_cols_to_import = [
    ONSClean.postcode,
    *contemporary_geography_columns,
    *current_geography_columns,
]

logger = logger.get_logger(__name__)


def main(
    cqc_locations_source: str,
    cleaned_ons_source: str,
    cleaned_cqc_locations_destination: str,
    gac_service_destination: str,
    regulated_activities_destination: str,
    specialisms_destination: str,
    postcode_matching_destination: str,
) -> None:
    """
    Performs cleaning and imputation steps for CQC location data.

    Filtered for:
    - location type is Social Care
    - registration status is registered
    - location has a regulated activity at some point
    - location GAC service type is not only Specialist College

    Coerced data types:
    - registrationDate
    - deregistrationDate

    Imputed columns:
    - registrationDate (forwards only)
    - providerId (forwards only)
    - regulatedActivities (forwards and backwards)
    - specialisms (forwards and backwards)
    - gacServiceTypes (forwards and backwards)
    - postalcode/address information (forwards and backwards)

    Args:
        cqc_locations_source (str): S3 URI to read CQC locations raw data from
        cleaned_ons_source (str): S3 URI to read ONS cleaned data from
        cleaned_cqc_locations_destination (str): S3 URI to WRITE CQC locations cleaned data to
        gac_service_destination (str): S3 URI to WRITE GAC services cleaned dimension to
        regulated_activities_destination (str): S3 URI to WRITE regulated activities cleaned dimension to
        specialisms_destination (str): S3 URI to WRITE specialisms cleaned dimension to
        postcode_matching_destination (str): S3 URI to WRITE postcode matching cleaned dimension to

    Raises:
        ColumnNotFoundError: When the schema has changed unexpectedly
        ClientError: When reading from or writing to S3 fails
        OSError: When there are no AWS credentials in the environment
        Exception: For any other error occurring during cleaning.
    """
    try:
        cqc_lf = utils.scan_parquet(
            cqc_locations_source,
            schema=POLARS_LOCATION_SCHEMA,
            selected_columns=cqc_location_cols_to_import,
        )

        logger.info("CQC Location LazyFrame read in")
        logger.debug(
            f"CQC Location LazyFrame has {cqc_lf.select(pl.len()).collect().item()} rows"
        )

        # Format dates
        cqc_lf = cqc_lf.with_columns(
            pl.col(CQCLClean.registration_date).str.to_date("%Y-%m-%d"),
            pl.col(CQCLClean.deregistration_date)
            .str.to_date("%Y-%m-%d", strict=False)
            .fill_null(
                pl.col(CQCLClean.deregistration_date).str.to_date(
                    "%Y%m%d", strict=False
                )
            ),
            pl.col(Keys.import_date).cast(pl.String).alias(Keys.import_date),
            pl.col(Keys.import_date)
            .cast(pl.String)
            .str.to_date("%Y%m%d")
            .alias(CQCLClean.cqc_location_import_date),
            pl.col(Keys.month).cast(pl.String).str.pad_start(2, "0"),
            pl.col(Keys.day).cast(pl.String).str.pad_start(2, "0"),
        )
        cqc_lf = clean_and_impute_registration_date(cqc_lf)

        # Clean provider ID, and then filter only for rows that have a provider id
        cqc_lf = clean_provider_id_column(cqc_lf)
        cqc_lf = cqc_lf.filter(pl.col(CQCLClean.provider_id).is_not_null())
        # Use the provider ID to identify which locations are members of the local authority
        cqc_lf = assign_cqc_sector(
            cqc_lf=cqc_lf, la_provider_ids=LocalAuthorityProviderIds.known_ids
        )

        # Filter CQC dataframe on known conditions
        cqc_lf = cqc_lf.filter(
            pl.col(CQCLClean.type).eq(LocationType.social_care_identifier),
            raw_data_adjustments.is_valid_location(),
        )
        logger.info("CQC Location LazyFrame filtered to registered Social Care Orgs")
        logger.debug(
            f"CQC Location LazyFrame has {cqc_lf.select(pl.len()).collect().item()} rows"
        )

        cqc_lf = impute_historic_relationships(cqc_lf)
        cqc_lf = select_registered_locations(cqc_lf)
        cqc_lf = add_related_location_flag(cqc_lf)

        # Calculate latest import date for dimension update date
        dimension_update_date = cqc_lf.select(Keys.import_date).max().collect().item()

        # Create Regulated Activities dimension delta
        regulated_activity_delta = create_dimension_from_struct_field(
            cqc_lf=cqc_lf,
            struct_column_name=CQCLClean.regulated_activities,
            dimension_location=regulated_activities_destination,
            dimension_update_date=dimension_update_date,
        )

        cqc_lf, regulated_activity_delta = (
            remove_locations_without_regulated_activities(
                cqc_lf=cqc_lf, regulated_activities_dimension=regulated_activity_delta
            )
        )
        logger.info(
            "CQC Location LazyFrame filtered to remove locations which have never had a regulated activity"
        )
        logger.debug(
            f"CQC Location LazyFrame has {cqc_lf.select(pl.len()).collect().item()} rows"
        )

        regulated_activity_delta = extract_registered_manager_names(
            regulated_activity_delta
        )

        utils.write_to_parquet(
            df=regulated_activity_delta.drop(
                CQCLClean.cqc_location_import_date
            ).collect(),
            output_path=regulated_activities_destination,
            logger=logger,
            partition_cols=dimension_partition_keys,
        )
        del regulated_activity_delta

        # Create Specialisms dimension
        specialisms_delta = create_dimension_from_struct_field(
            cqc_lf=cqc_lf,
            struct_column_name=CQCLClean.specialisms,
            dimension_location=specialisms_destination,
            dimension_update_date=dimension_update_date,
        )
        # Extract and categorise the location specialisms
        specialisms_delta = specialisms_delta.with_columns(
            pl.col(CQCLClean.imputed_specialisms)
            .list.eval(pl.element().struct.field(CQCLClean.name))
            .alias(CQCLClean.specialisms_offered)
        )
        specialisms_delta = assign_specialism_category(
            lf=specialisms_delta, specialism=Specialisms.dementia
        )
        specialisms_delta = assign_specialism_category(
            lf=specialisms_delta, specialism=Specialisms.learning_disabilities
        )
        specialisms_delta = assign_specialism_category(
            lf=specialisms_delta, specialism=Specialisms.mental_health
        )
        utils.write_to_parquet(
            df=specialisms_delta.drop(CQCLClean.cqc_location_import_date).collect(),
            output_path=specialisms_destination,
            logger=logger,
            partition_cols=dimension_partition_keys,
        )
        del specialisms_delta

        # Create GAC Service dimension delta
        gac_service_delta = create_dimension_from_struct_field(
            cqc_lf=cqc_lf,
            struct_column_name=CQCLClean.gac_service_types,
            dimension_location=gac_service_destination,
            dimension_update_date=dimension_update_date,
        )

        gac_service_delta = gac_service_delta.with_columns(
            pl.col(CQCLClean.imputed_gac_service_types)
            .list.eval(pl.element().struct.field(CQCLClean.description))
            .alias(CQCLClean.services_offered)
        )

        cqc_lf, gac_service_delta = remove_specialist_colleges(
            cqc_lf=cqc_lf, gac_services_dimension=gac_service_delta
        )

        gac_service_delta = assign_primary_service_type(gac_service_delta)
        gac_service_delta = assign_care_home(gac_service_delta)

        utils.write_to_parquet(
            df=gac_service_delta.drop(CQCLClean.cqc_location_import_date).collect(),
            output_path=gac_service_destination,
            logger=logger,
            partition_cols=dimension_partition_keys,
        )
        del gac_service_delta

        # Create postcode matching dimension
        ons_lf = utils.scan_parquet(
            cleaned_ons_source, selected_columns=ons_cols_to_import
        )
        logger.info("Cleaned ONS LazyFrame read in")
        logger.debug(
            f"Cleaned ONS LazyFrame has {ons_lf.select(pl.len()).collect().item()} rows"
        )

        postcode_delta = create_dimension_from_postcode(
            cqc_lf=cqc_lf,
            ons_lf=ons_lf,
            dimension_location=postcode_matching_destination,
            dimension_update_date=dimension_update_date,
        )
        utils.write_to_parquet(
            df=postcode_delta.drop(CQCLClean.cqc_location_import_date).collect(),
            output_path=postcode_matching_destination,
            logger=logger,
            partition_cols=dimension_partition_keys,
        )
        del postcode_delta

        # Drop columns stored in dimensions
        cqc_lf = cqc_lf.drop(
            # From RegulatedActivities dimension
            CQCLClean.regulated_activities,
            # From Specialisms dimension
            CQCLClean.specialisms,
            # From GAC Services dimension
            CQCLClean.gac_service_types,
            CQCLClean.care_home,
            # From PostcodeMatching dimension
            CQCLClean.postal_code,
            CQCLClean.postal_address_line1,
        )

        utils.write_to_parquet(
            df=cqc_lf.collect(),
            output_path=cleaned_cqc_locations_destination,
            logger=logger,
            partition_cols=cqc_partition_keys,
        )
    except ColumnNotFoundError as e:
        logger.error("There has been an unexpected schema change.")
        logger.error(sys.argv)
        logger.error(e)
        raise
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            logger.error(
                "Please check you are reading from/writing to the right bucket or update your IAM permissions"
            )
        elif e.response["Error"]["Code"] == "NoSuchKey":
            logger.error("The file does not exist, please check the path.")
        logger.error(sys.argv)
        logger.error(e)
        raise
    except OSError as e:
        if "Generic S3 error" in str(e):
            logger.error(
                "There are no AWS credentials available. If running locally, please ensure you are authenticated before running."
            )
        logger.error(sys.argv)
        logger.error(e)
        raise
    except Exception as e:
        logger.error(sys.argv)
        logger.error(e)
        raise


def create_dimension_from_struct_field(
    cqc_lf: pl.LazyFrame,
    struct_column_name: str,
    dimension_location: str,
    dimension_update_date: str,
) -> pl.LazyFrame:
    """
    Creates a dimension for a struct column by imputing missing values from history, then from the future for each location id.

    1. Uses the value in the existing column for 'imputed_[column_name]' if it is a list which contains values.
    2. First forward and then backwards fill any missing values in 'imputed_[column_name]' for each location id
    3. Create dimension delta, including rows from any new import dates, as well as any updated values for old import dates


    Args:
        cqc_lf (pl.LazyFrame): Dataframe containing 'location_id', 'cqc_location_import_date', 'import_date' and a struct column to impute.
        struct_column_name (str): Name of the struct column to impute.
        dimension_location (str): Location of the dimension data
        dimension_update_date (str): Update date of the dimension date

    Returns:
        pl.LazyFrame:  Dataframe of delta dimension table, with rows of the changes since the last update.
    """
    # 1. Uses the value in the existing column for 'imputed_[column_name]' if it is a list which contains values.
    imputed_column_name = "imputed_" + struct_column_name
    current_dim = cqc_lf.select(
        CQCLClean.location_id,
        struct_column_name,
        CQCLClean.cqc_location_import_date,
        Keys.import_date,
    )
    current_dim = current_dim.with_columns(
        pl.when(pl.col(struct_column_name).list.len() > 0)
        .then(pl.col(struct_column_name))
        .otherwise(None)
        .alias(imputed_column_name)
    )

    # 2. First forward and then backwards fill any missing values in 'imputed_[column_name]' for each location id
    current_dim = current_dim.with_columns(
        pl.col(imputed_column_name)
        .forward_fill()
        .backward_fill()
        .over(
            partition_by=CQCLClean.location_id,
            order_by=CQCLClean.cqc_location_import_date,
        )
    )

    # 3. Create dimension delta, including rows from any new import dates, as well as any updated values for old import dates
    return _create_dimension_delta(
        dimension_location=dimension_location,
        dimension_update_date=dimension_update_date,
        current_dimension=current_dim,
        join_columns=[
            CQCLClean.location_id,
            struct_column_name,
            imputed_column_name,
            Keys.import_date,
        ],
    )


def create_dimension_from_postcode(
    cqc_lf: pl.LazyFrame,
    ons_lf: pl.LazyFrame,
    dimension_location: str,
    dimension_update_date: str,
):
    """
    Creates dimension from postcode column, matching ONS data
    Args:
        cqc_lf (pl.LazyFrame): LazyFrame containing 'location_id', 'cqc_location_import_date', 'import_date' and 'postalcode'.
        ons_lf (pl.LazyFrame): LazyFrame containing ONS data
        dimension_location (str): Location of the dimension data
        dimension_update_date (str): Update date of the dimension date

    Returns:
        pl.LazyFrame:  Dataframe of delta dimension table, with rows of the changes since the last update.
    """
    postcode_columns = [
        CQCLClean.location_id,
        CQCLClean.name,
        CQCLClean.cqc_location_import_date,
        CQCLClean.postal_address_line1,
        CQCLClean.postal_code,
        Keys.import_date,
    ]
    if not all(col in cqc_lf.columns for col in postcode_columns):
        raise ColumnNotFoundError(
            "One or more required columns are missing from the CQC dataframe. "
            f"\nRequired columns: {postcode_columns}"
            f"\nExisting columns: {cqc_lf.columns}"
        )

    current_dim = run_postcode_matching(
        cqc_lf.select(
            CQCLClean.location_id,
            CQCLClean.name,
            CQCLClean.cqc_location_import_date,
            CQCLClean.postal_address_line1,
            CQCLClean.postal_code,
            Keys.import_date,
        ),
        ons_lf,
    )

    return _create_dimension_delta(
        dimension_location=dimension_location,
        dimension_update_date=dimension_update_date,
        current_dimension=current_dim,
        join_columns=[
            CQCLClean.location_id,
            CQCLClean.postcode_cleaned,
            CQCLClean.cqc_location_import_date,
        ],
    )


def _create_dimension_delta(
    dimension_location: str,
    dimension_update_date: str,
    current_dimension: pl.LazyFrame,
    join_columns: list[str],
) -> pl.LazyFrame:
    """
    Create dimension delta, including rows from any new import dates, as well as any updated values for old import dates

    1. Read in the previous state of the dimension.
        - If no such dimension exists, create a new one.
    2. Identify which rows in the current dimension are new or updated.
    3. Assign partition values to the updated rows.

    Args:
        dimension_location (str): Location of the (historic) dimension data
        dimension_update_date (str): Update date of the dimension date (where the dimension is/will be stored)
        current_dimension (pl.LazyFrame): Current dimension data
        join_columns (list[str]): List of columns to join current dimension data to historic dimension data

    Returns:
        pl.LazyFrame: Dataframe of delta dimension table, with rows of the changes since the last update.

    """
    # 1. Read in the previous state of the dimension.
    dimension_name = dimension_location.split("/")[-2]
    try:
        previous_dimension = utils.scan_parquet(dimension_location).with_columns(
            pl.col(DimensionKeys.import_date).cast(pl.String)
        )
    except FileNotFoundError:
        warnings.warn(
            f"The {dimension_name} dimension was not found in the {dimension_location}. A new dimension will be created.",
            UserWarning,
        )
        delta = current_dimension
    else:
        # 2. Identify which rows in the current dimension are new or updated.
        delta = current_dimension.join(
            previous_dimension,
            on=join_columns,
            how="anti",
            nulls_equal=True,
        )

    # 3. Assign partition values to the updated rows.
    delta = delta.with_columns(
        pl.lit(dimension_update_date[:4]).alias(DimensionKeys.year),
        pl.lit(dimension_update_date[4:6]).alias(DimensionKeys.month),
        pl.lit(dimension_update_date[6:]).alias(DimensionKeys.day),
        pl.lit(dimension_update_date).alias(DimensionKeys.last_updated),
    )
    logger.info(f"The {dimension_name} delta has been created.")
    logger.debug(
        f"{dimension_name} delta LazyFrame has {delta.select(pl.len()).collect().item()} rows"
    )

    return delta


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
    # 1. Replace provider ids with more than 14 characters with Null
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.provider_id).str.len_chars() <= 14)
        .then(pl.col(CQCLClean.provider_id))
        .otherwise(None)
        .alias(CQCLClean.provider_id)
    )

    # 2. Forward and backwards fill missing provider ids over location id
    cqc_lf = cqc_lf.with_columns(
        pl.col(CQCLClean.provider_id)
        .forward_fill()
        .backward_fill()
        .over(CQCLClean.location_id)
    )
    return cqc_lf


def clean_and_impute_registration_date(
    cqc_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Cleans registration dates into YYYY-MM-DD format, removing invalid dates and then imputing missing values.

    1. Copy all existing registration dates into the imputed column
    2. Remove any time elements from the (imputed) registration date
    3. Replace registration dates that are after the import date with null
    4. Replace registration dates with the minimum registration date for that location id
    5. Replace registration dates with the minimum import date if there are no registration dates for that location

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with registration and import date columns

    Returns:
        pl.LazyFrame: Dataframe with imputed registration date column

    """
    # 1. Copy all existing registration dates into the imputed column
    cqc_lf = cqc_lf.with_columns(
        pl.col(CQCLClean.registration_date).alias(CQCLClean.imputed_registration_date)
    )

    # 2. Remove any time elements from the (imputed) registration date
    cqc_lf = cqc_lf.with_columns(pl.col(CQCLClean.imputed_registration_date).dt.date())

    # 3. Replace registration dates that are after the import date with null
    cqc_lf = cqc_lf.with_columns(
        pl.when(
            pl.col(CQCLClean.imputed_registration_date)
            <= pl.col(Keys.import_date).str.to_date(format="%Y%m%d")
        )
        .then(pl.col(CQCLClean.imputed_registration_date))
        .otherwise(None)
    )

    # 4. Replace registration dates with the minimum registration date for that location id
    cqc_lf = cqc_lf.with_columns(
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
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.imputed_registration_date).is_null())
        .then(
            pl.col(Keys.import_date)
            .min()
            .over(CQCLClean.location_id)
            .str.strptime(pl.Date, format="%Y%m%d")
        )
        .otherwise(pl.col(CQCLClean.imputed_registration_date))
        .alias(CQCLClean.imputed_registration_date)
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


def assign_primary_service_type(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Allocates the primary service type for each row in the LazyFrame based on the descriptions in the 'imputed_gac_service_types' field.

    1. If any of the imputed GAC service descriptions have "Care home service with nursing" allocate as "Care home with nursing"
    2. If not, if any of the imputed GAC service descriptions have "Care home service without nursing" allocate as "Care home without nursing"
    3. Otherwise, allocate as "non-residential"

    Args:
        cqc_lf (pl.LazyFrame): The input LazyFrame containing the 'imputed_gac_service_types' column.

    Returns:
        pl.LazyFrame: Dataframe with the new 'primary_service_type' column.

    """
    # 1. If any of the imputed GAC service descriptions have "Care home service with nursing" allocate as "Care home with nursing"
    cqc_lf = cqc_lf.with_columns(
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

    return cqc_lf


def assign_care_home(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Assigns care home status for each row based on the Primary Service Type

    1. If the Primary Service Type is 'Care home service with nursing' or 'Care home only' then assign 'Y'
    2. Otherwise, assign 'N'

    Args:
        cqc_lf (pl.LazyFrame): LazyFrame containing 'primary_service_type' column.

    Returns:
        pl.LazyFrame: Dataframe with the new 'care_home' column.

    """
    cqc_lf = cqc_lf.with_columns(
        # 1. If the Primary Service Type is 'Care home service with nursing' or 'Care home only' then assign 'Y'
        pl.when(
            pl.col(CQCLClean.primary_service_type).is_in(
                [
                    PrimaryServiceType.care_home_with_nursing,
                    PrimaryServiceType.care_home_only,
                ]
            )
        )
        .then(pl.lit(CareHome.care_home))
        # 2. Otherwise, assign 'N'
        .otherwise(pl.lit(CareHome.not_care_home))
        .alias(CQCLClean.care_home)
    )
    return cqc_lf


def add_related_location_flag(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a column which flags whether the location was related to a previous location or not

    1. If the length of imputed relationships is more than 0 then flag 'Y'
    2. Otherwise, flag 'N'

    Args:
        cqc_lf (pl.LazyFrame): A dataframe with the imputed_relationships column.

    Returns:
        pl.LazyFrame: Dataframe with an added related_location column.
    """
    cqc_lf = cqc_lf.with_columns(
        # 1. If the length of imputed relationships is more than 0 then flag 'Y'
        pl.when(pl.col(CQCLClean.imputed_relationships).list.len() > 0)
        .then(pl.lit(RelatedLocation.has_related_location))
        # 2. Otherwise, flag 'N'
        .otherwise(pl.lit(RelatedLocation.no_related_location))
        .alias(CQCLClean.related_location)
    )

    return cqc_lf


def remove_specialist_colleges(
    cqc_lf: pl.LazyFrame, gac_services_dimension: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    We do not include locations which are only specialist colleges in our
    estimates. This function identifies and removes the ones listed in the locations dataset.

    1. Filter for rows in the GAC Service dimension where "Specialist college service" is the only service offered
    2. Remove the identified rows from the cqc fact table, and the GAC Service Dimension

    Args:
        cqc_lf (pl.LazyFrame): Fact table to align with dimension
        gac_services_dimension (pl.LazyFrame): Dimension table with services_offered column

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: cqq_lf, gac_services_dimension with locations which are only specialist colleges removed.

    """
    to_remove_lf = gac_services_dimension.filter(
        # 1. Filter for rows in the GAC Service dimension where "Specialist college service" is the only service offered
        pl.col(CQCLClean.services_offered)
        .list.first()
        .eq(Services.specialist_college_service),
        pl.col(CQCLClean.services_offered).list.len().eq(1),
        pl.col(CQCLClean.services_offered).is_not_null(),
    ).select(CQCLClean.location_id, Keys.import_date)

    # 2. Remove the identified rows from the cqc fact table, and the GAC Service Dimension
    cqc_lf, gac_services_dimension = remove_rows(
        to_remove_lf=to_remove_lf, target_lfs=[cqc_lf, gac_services_dimension]
    )
    return cqc_lf, gac_services_dimension


def remove_locations_without_regulated_activities(
    cqc_lf: pl.LazyFrame, regulated_activities_dimension: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    Remove locations that have no imputed regulated activities. This should only be the case when the location has never had any reported regulated activities.

    1. Filter for rows in the Regulated Activities dimension where there are no imputed regulated activities.
    2. Check that none of these locations have regulated activities for any other date.
    3. Remove the identified rows from the cqc fact table, and the Regulated Activities dimension

    Args:
        cqc_lf (pl.LazyFrame): Fact table to align with dimension
        regulated_activities_dimension (pl.LazyFrame): Dimension table with imputed_regulated_activities column

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: cqq_lf, regulated_activities_dimension where all rows have imputed regulated activities.

    """
    # 1. Filter for rows in the Regulated Activities dimension where there are no imputed regulated activities.
    to_remove_lf = (
        regulated_activities_dimension.filter(
            pl.col(CQCLClean.imputed_regulated_activities).is_null()
            | pl.col(CQCLClean.imputed_regulated_activities).list.len().eq(0)
        )
        .select(CQCLClean.location_id)
        .unique()
    )

    # 2. Check that none of these locations have regulated activities for any other date.
    locations_to_investigate = regulated_activities_dimension.join(
        to_remove_lf,
        on=CQCLClean.location_id,
        how="semi",
    ).filter(pl.col(CQCLClean.imputed_regulated_activities).is_not_null())

    if not locations_to_investigate.collect().is_empty():
        warnings.warn(
            message=(
                "The following locations have some dates with imputed regulated activities, and others do not: "
                f"{locations_to_investigate.select(CQCLClean.location_id).collect().get_column(CQCLClean.location_id).unique().to_list()}. "
                "Please check that the imputation has been carried out correctly."
            ),
            category=UserWarning,
        )

    # 3. Remove the identified rows from the cqc fact table, and the Regulated Activities dimension
    cqc_lf, regulated_activities_dimension = remove_rows(
        to_remove_lf=to_remove_lf, target_lfs=[cqc_lf, regulated_activities_dimension]
    )
    return cqc_lf, regulated_activities_dimension


def remove_rows(
    to_remove_lf: pl.LazyFrame, target_lfs: list[pl.LazyFrame]
) -> list[pl.LazyFrame]:
    """
    Remove rows from a fact table and any provided dimension tables
    Args:
        to_remove_lf (pl.LazyFrame): Dataframe with rows to remove (all columns present will be used as keys for the anti-join).
        target_lfs (list[pl.LazyFrame]): Target tables from which to remove the rows.

    Returns:
        list[pl.LazyFrame]: List of dataframes in the same order as target_lfs, with the rows removed.

    Raises:
        ValueError: If any of the target_lfs does not contain the columns in to_remove_lf.
    """
    result_lfs = []
    to_remove_schema = set(to_remove_lf.head(1).collect().schema.to_python())
    for target_lf in target_lfs:
        target_lf_schema = set(target_lf.head(1).collect().schema.to_python())
        if not to_remove_schema.issubset(target_lf_schema):
            raise ValueError(
                "The target dataframe schema does not contain all the columns present to_remove_lf, or the types are not matched."
                f"\nto_remove_schema: {to_remove_schema}"
                f"\ntarget_lf_schema: {target_lf_schema}"
            )
        result_lfs.append(
            target_lf.join(
                to_remove_lf,
                on=to_remove_lf.columns,
                how="anti",
            )
        )

    return result_lfs


def select_registered_locations(cqc_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Select rows where registration status is registered.

    1. Check that there are no values in registration status that is not registered or deregistered.
    2. Select rows where registration status is registered.

    Args:
        cqc_lf (pl.LazyFrame): Dataframe filter

    Returns:
        pl.LazyFrame: Dataframe with only rows where registration status is registered.
    """
    # 1. Check that there are no values in registration status that is not registered or deregistered.
    invalid_rows = cqc_lf.filter(
        ~pl.col(CQCLClean.registration_status).is_in(
            [RegistrationStatus.registered, RegistrationStatus.deregistered]
        )
    )

    if not invalid_rows.collect().is_empty():
        warnings.warn(
            (
                f"{invalid_rows.select(pl.len()).collect().item()} row(s) had an invalid registration status and have been dropped."
                "\nThe following values are invalid:"
                f"{invalid_rows.select(CQCLClean.registration_status).collect().get_column(CQCLClean.registration_status).value_counts()}"
            ),
            UserWarning,
        )

    # 2. Select rows where registration status is registered.
    cqc_lf = cqc_lf.filter(
        pl.col(CQCLClean.registration_status).eq(RegistrationStatus.registered)
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
        # 1. If the Provider ID is in the list of la_provider_ids then assign "Local authority"
        pl.when(pl.col(CQCLClean.provider_id).is_in(la_provider_ids))
        .then(pl.lit(Sector.local_authority))
        # 2. Otherwise, assign "Independent"
        .otherwise(pl.lit(Sector.independent))
        .alias(CQCLClean.cqc_sector)
    )
    return cqc_lf


def assign_specialism_category(lf: pl.LazyFrame, specialism: str) -> pl.LazyFrame:
    """
    Categorises each row as "Specialist", "Generalist" or "Other for the given specialism.

    1. If the specialism is the only one offered by the location then categorise as "Specialist".
    2. If the specialism among multiple offered by the location then categorise as "Generalist".
    3. Otherwise, categorise as "Specialist".

    Args:
        lf (pl.LazyFrame): Dataframe with specialisms_offered column.
        specialism (str): Specialism to categorise.

    Returns:
        pl.LazyFrame: Input dataframe with new "specialist_generalist_other_[specialism]" column.

    """
    new_column_name: str = f"specialist_generalist_other_{specialism}".replace(
        " ", "_"
    ).lower()

    lf = lf.with_columns(
        # 1. If the specialism is the only one offered by the location then categorise as "Specialist".
        pl.when(
            pl.col(CQCLClean.specialisms_offered).list.contains(specialism),
            pl.col(CQCLClean.specialisms_offered).list.len() == 1,
        )
        .then(pl.lit(SpecialistGeneralistOther.specialist))
        # 2. If the specialism among multiple offered by the location then categorise as "Generalist".
        .when(pl.col(CQCLClean.specialisms_offered).list.contains(specialism))
        .then(pl.lit(SpecialistGeneralistOther.generalist))
        # 3. Otherwise, categorise as "Specialist".
        .otherwise(pl.lit(SpecialistGeneralistOther.other))
        .alias(new_column_name)
    )
    return lf


if __name__ == "__main__":
    args = utils.get_args(
        ("--cqc_locations_source", "S3 URI to read CQC locations raw data from"),
        ("--cleaned_ons_source", "S3 URI to read ONS cleaned data from"),
        (
            "--cleaned_cqc_locations_destination",
            "S3 URI to WRITE CQC locations cleaned data to",
        ),
        (
            "--gac_service_destination",
            "S3 URI to WRITE GAC services cleaned dimension to",
        ),
        (
            "--regulated_activities_destination",
            "S3 URI to WRITE regulated activities cleaned dimension to",
        ),
        (
            "--specialisms_destination",
            "S3 URI to WRITE specialisms cleaned dimension to",
        ),
        (
            "--postcode_matching_destination",
            "S3 URI to WRITE postcode matching cleaned dimension to",
        ),
    )
    logger.info("Running cleaning job")

    main(
        cqc_locations_source=args.cqc_locations_source,
        cleaned_ons_source=args.cleaned_ons_source,
        cleaned_cqc_locations_destination=args.cleaned_cqc_locations_destination,
        gac_service_destination=args.gac_service_destination,
        regulated_activities_destination=args.regulated_activities_destination,
        specialisms_destination=args.specialisms_destination,
        postcode_matching_destination=args.postcode_matching_destination,
    )

    logger.info("Finished cleaning job")
