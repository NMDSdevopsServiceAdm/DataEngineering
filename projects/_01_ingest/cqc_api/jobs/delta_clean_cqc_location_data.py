import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import utils.cleaning_utils as cUtils
from projects._01_ingest.cqc_api.utils.extract_registered_manager_names import (
    extract_registered_manager_names,
)
from projects._01_ingest.cqc_api.utils.postcode_matcher import run_postcode_matching
from projects._01_ingest.cqc_api.utils.utils import classify_specialisms
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    DimensionPartitionKeys as DimensionKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    RegistrationStatus,
    RelatedLocation,
    Sector,
    Services,
    Specialisms,
)
from utils.cqc_local_authority_provider_ids import LocalAuthorityProviderIds
from utils.raw_data_adjustments import remove_records_from_locations_data

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]
dimensionPartitionKeys = [
    DimensionKeys.year,
    DimensionKeys.month,
    DimensionKeys.day,
    DimensionKeys.last_updated,
    DimensionKeys.import_date,
]

cqc_location_api_cols_to_import = [
    CQCL.location_id,
    CQCL.provider_id,
    CQCL.name,
    CQCL.postal_address_line1,
    CQCL.postal_code,
    CQCL.registration_status,
    CQCL.registration_date,
    CQCL.deregistration_date,
    CQCL.type,
    CQCL.relationships,
    CQCL.care_home,
    CQCL.number_of_beds,
    CQCL.dormancy,
    CQCL.gac_service_types,
    CQCL.regulated_activities,
    CQCL.specialisms,
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


def main(
    cqc_location_source: str,
    cleaned_ons_postcode_directory_source: str,
    manual_postcode_corrections_source: str,
    cleaned_cqc_location_destination: str,
    gac_service_destination: str,
    regulated_activities_destination: str,
    specialisms_destination: str,
    postcode_matching_destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cqc_location_source, selected_columns=cqc_location_api_cols_to_import
    )

    ons_postcode_directory_df = utils.read_from_parquet(
        cleaned_ons_postcode_directory_source, selected_columns=ons_cols_to_import
    )

    cqc_location_df = create_cleaned_registration_date_column(cqc_location_df)
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, CQCLClean.imputed_registration_date, string_format="yyyy-MM-dd"
    )

    cqc_location_df = clean_provider_id_column(cqc_location_df)
    cqc_location_df = utils.select_rows_with_non_null_value(
        cqc_location_df, CQCL.provider_id
    )
    cqc_location_df = utils.select_rows_with_non_null_value(
        cqc_location_df, CQCL.registration_status
    )
    cqc_location_df = utils.select_rows_with_non_null_value(cqc_location_df, CQCL.type)

    known_la_providerids = LocalAuthorityProviderIds.known_ids
    cqc_location_df = add_cqc_sector_column_to_cqc_locations_dataframe(
        cqc_location_df, known_la_providerids
    )

    cqc_location_df = remove_records_from_locations_data(cqc_location_df)

    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=CQCLClean.registration_date,  # This will format both registration date and deregistration date
        raw_date_format="yyyy-MM-dd",
    )
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    cqc_location_df = impute_historic_relationships(cqc_location_df)

    dimension_update_date = cqc_location_df.agg(F.max(Keys.import_date)).collect()[0][0]

    # Create regulated activity dimension
    regulated_activity_delta = create_dimension_from_missing_struct_column(
        cqc_location_df,
        CQCL.regulated_activities,
        regulated_activities_destination,
        dimension_update_date,
    )
    cqc_location_df = cqc_location_df.drop(CQCL.regulated_activities)

    cqc_location_df, regulated_activity_delta = (
        remove_locations_that_never_had_regulated_activities(
            cqc_location_df, regulated_activity_delta
        )
    )
    regulated_activity_delta = extract_registered_manager_names(
        regulated_activity_delta
    ).drop(CQCLClean.cqc_location_import_date)

    utils.write_to_parquet(
        regulated_activity_delta,
        output_dir=regulated_activities_destination,
        mode="append",
        partitionKeys=dimensionPartitionKeys,
    )

    # Create specialisms dimension
    specialisms_delta = create_dimension_from_missing_struct_column(
        cqc_location_df,
        CQCL.specialisms,
        specialisms_destination,
        dimension_update_date,
    )
    cqc_location_df = cqc_location_df.drop(CQCL.specialisms)

    specialisms_delta = extract_from_struct(
        specialisms_delta,
        specialisms_delta[CQCLClean.imputed_specialisms][CQCL.name],
        CQCLClean.specialisms_offered,
    )
    specialisms_delta = classify_specialisms(
        specialisms_delta,
        Specialisms.dementia,
    )
    specialisms_delta = classify_specialisms(
        specialisms_delta,
        Specialisms.learning_disabilities,
    )
    specialisms_delta = classify_specialisms(
        specialisms_delta,
        Specialisms.mental_health,
    ).drop(CQCLClean.cqc_location_import_date)

    utils.write_to_parquet(
        specialisms_delta,
        output_dir=specialisms_destination,
        mode="append",
        partitionKeys=dimensionPartitionKeys,
    )

    # Create GAC service dimension
    gac_service_delta = create_dimension_from_missing_struct_column(
        cqc_location_df,
        CQCL.gac_service_types,
        gac_service_destination,
        dimension_update_date,
    )
    # Drop care home from registered location df - stored in gac service dimension
    cqc_location_df = cqc_location_df.drop(CQCL.gac_service_types, CQCL.care_home)

    gac_service_delta = extract_from_struct(
        gac_service_delta,
        gac_service_delta[CQCLClean.imputed_gac_service_types][CQCL.description],
        CQCLClean.services_offered,
    )

    gac_service_delta = allocate_primary_service_type(gac_service_delta)

    gac_service_delta = realign_carehome_column_with_primary_service(
        gac_service_delta
    ).drop(CQCLClean.cqc_location_import_date)

    utils.write_to_parquet(
        gac_service_delta,
        output_dir=gac_service_destination,
        mode="append",
        partitionKeys=dimensionPartitionKeys,
    )

    # Final cleaning on fact table
    cqc_location_df = add_related_location_column(cqc_location_df)

    # Create postcode matching dimension
    postcode_matching_delta = create_postcode_matching_dimension(
        cqc_location_df,
        ons_postcode_directory_df,
        postcode_matching_destination,
        dimension_update_date,
        manual_postcode_corrections_source,
    )

    cqc_location_df = cqc_location_df.drop(CQCL.postal_code, CQCL.postal_address_line1)

    utils.write_to_parquet(
        postcode_matching_delta,
        output_dir=postcode_matching_destination,
        mode="append",
        partitionKeys=dimensionPartitionKeys,
    )

    utils.write_to_parquet(
        cqc_location_df,
        cleaned_cqc_location_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


def create_postcode_matching_dimension(
    cqc_df: DataFrame,
    postcode_df: DataFrame,
    dimension_location: str,
    dimension_update_date: str,
    manual_postcode_corrections_source: str,
) -> DataFrame:
    """
    Creates (or updates) the postcode matching dimension by comparing current CQC data
    against the existing dimension and appending only new records.

    This function reads any existing dimension from the given location, generates a
    current dimension using postcode matching logic, identifies new rows not present
    in the previous dimension (if available), and adds metadata columns for
    date-based partitioning.

    Args:
        cqc_df (DataFrame): DataFrame containing CQC location data.
        postcode_df (DataFrame): DataFrame containing postcode data used for matching.
        dimension_location (str): S3 path where the dimension data should be stored
            dimension parquet file is stored.
        dimension_update_date (str): Date string in 'YYYYMMDD' format for creating partition keys.
        manual_postcode_corrections_source (str): The s3 URI for the incorrect postcode csv.

    Returns:
        DataFrame: Dataframe of delta dimension table, with rows of the changes since the last update.
    """
    try:
        previous_dimension = utils.read_from_parquet(dimension_location)
    except AnalysisException:
        print(
            f"The postcode dimension was not found in {dimension_location}. A new dimension will be created."
        )
        previous_dimension = None

    current_dimension = run_postcode_matching(
        cqc_df.select(
            CQCL.location_id,
            CQCL.name,
            CQCLClean.cqc_location_import_date,
            CQCL.postal_address_line1,
            CQCL.postal_code,
            Keys.import_date,
        ),
        postcode_df,
        manual_postcode_corrections_source,
    )

    if previous_dimension:
        delta = current_dimension.join(
            previous_dimension,
            on=[
                CQCLClean.location_id,
                CQCLClean.postcode_cleaned,
                CQCLClean.cqc_location_import_date,
            ],
            how="anti",
        )

    else:
        delta = current_dimension

    delta = (
        delta.withColumn(DimensionKeys.last_updated, F.lit(dimension_update_date))
        .withColumn(DimensionKeys.year, F.lit(dimension_update_date[:4]))
        .withColumn(DimensionKeys.month, F.lit(dimension_update_date[4:6]))
        .withColumn(DimensionKeys.day, F.lit(dimension_update_date[6:]))
        .drop(CQCL.name)
    )

    return delta


def create_dimension_from_missing_struct_column(
    df: DataFrame,
    missing_struct_column: str,
    dimension_location: str,
    dimension_update_date: str,
) -> DataFrame:
    """
    Creates delta dimension table for a given missing struct column.
    Args:
        df (DataFrame): Dataframe with column which has missing structs
        missing_struct_column (str): Name of missing struct column
        dimension_location (str): Path that dimension is stored in
        dimension_update_date (str): Date that delta data will be stored in

    Returns:
        DataFrame: Dataframe of delta dimension table, with rows of the changes since the last update.
    """
    try:
        previous_dimension = utils.read_from_parquet(dimension_location)
    except AnalysisException:
        print(
            f"The {missing_struct_column} dimension was not found in the {dimension_location}. A new dimension will be created."
        )
        previous_dimension = None

    current_dimension = impute_missing_struct_column(
        df.select(
            CQCL.location_id,
            missing_struct_column,
            CQCLClean.cqc_location_import_date,
            Keys.import_date,
        ),
        missing_struct_column,
    ).select(
        CQCLClean.location_id,
        missing_struct_column,
        "imputed_" + missing_struct_column,
        CQCLClean.cqc_location_import_date,
        Keys.import_date,
    )

    if previous_dimension:
        delta = current_dimension.join(
            previous_dimension,
            on=[
                CQCLClean.location_id,
                missing_struct_column,
                "imputed_" + missing_struct_column,
                Keys.import_date,
            ],
            how="anti",
        )
    else:
        delta = current_dimension

    delta = (
        delta.withColumn(DimensionKeys.last_updated, F.lit(dimension_update_date))
        .withColumn(DimensionKeys.year, F.lit(dimension_update_date[:4]))
        .withColumn(DimensionKeys.month, F.lit(dimension_update_date[4:6]))
        .withColumn(DimensionKeys.day, F.lit(dimension_update_date[6:]))
    )

    return delta


def clean_provider_id_column(cqc_df: DataFrame) -> DataFrame:
    cqc_df = remove_provider_ids_with_too_many_characters(cqc_df)
    cqc_df = fill_missing_provider_ids_from_other_rows(cqc_df)
    return cqc_df


def remove_provider_ids_with_too_many_characters(cqc_df: DataFrame) -> DataFrame:
    cqc_df = cqc_df.withColumn(
        CQCL.provider_id,
        F.when(F.length(CQCL.provider_id) <= 14, cqc_df[CQCL.provider_id]),
    )
    return cqc_df


def fill_missing_provider_ids_from_other_rows(cqc_df: DataFrame) -> DataFrame:
    cqc_df = cqc_df.withColumn(
        CQCL.provider_id,
        F.when(
            cqc_df[CQCL.provider_id].isNull(),
            F.first(CQCL.provider_id, ignorenulls=True).over(
                Window.partitionBy(CQCL.location_id)
            ),
        ).otherwise(cqc_df[CQCL.provider_id]),
    )
    return cqc_df


# converted to polars -> projects\_01_ingest\cqc_api\fargate\utils\locations_4_clean_utils.py
# renamed as clean_and_impute_registration_date.
def create_cleaned_registration_date_column(cqc_df: DataFrame) -> DataFrame:
    """
    Adds a new column which is a cleaned and imputed copy of registrationdate.

    This function handles the steps for creating, cleaning and filling the blanks in the column imputed_registration_date.

    Args:
        cqc_df (DataFrame): A dataframe of CQC locations data with the column location_id, import_date, and registrationdate.

    Returns:
        DataFrame: A dataframe of CQC locations data with the additional column imputed_registration_date.
    """
    cqc_df = cqc_df.withColumn(
        CQCLClean.imputed_registration_date, cqc_df[CQCL.registration_date]
    )
    cqc_df = remove_time_from_date_column(cqc_df, CQCLClean.imputed_registration_date)
    cqc_df = remove_registration_dates_that_are_later_than_import_date(cqc_df)
    cqc_df = impute_missing_registration_dates(cqc_df)
    return cqc_df


def remove_time_from_date_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Converts a timestamp-as-string to a date-as-string.

    This function takes a column of string type containing mixed date and timestamp data and removes the time portion of the timestamp data.

    Args:
        df (DataFrame): A dataframe with the named column.
        column_name (str): A string with the name of the column to remove time data from.

    Returns:
        DataFrame: A dataframe with the named column without time data.
    """
    df = df.withColumn(column_name, F.substring(column_name, 1, 10))
    return df


def remove_registration_dates_that_are_later_than_import_date(
    df: DataFrame,
) -> DataFrame:
    """
    Nullifies registration dates from the imupted_registration_date column which are after the import date.

    This function changes registration dates from the imupted_registration_date column to None when they are greater than the import date.

    Args:
        df (DataFrame): A dataframe of CQC locations data with the column imputed_registration_date containing cleaned registration dates.

    Returns:
        DataFrame: A dataframe of CQC locations data with the column imputed_registration_date containing registration dates that are less than or equal to than the import date.
    """
    min_import_date = "min_import_date"
    df = df.withColumn(
        min_import_date,
        F.regexp_replace(
            F.min(Keys.import_date).over(Window.partitionBy(CQCL.location_id)),
            "(\d{4})(\d{2})(\d{2})",
            "$1-$2-$3",
        ),
    )
    df = df.withColumn(
        CQCLClean.imputed_registration_date,
        F.when(
            df[CQCLClean.imputed_registration_date] <= df[min_import_date],
            df[CQCLClean.imputed_registration_date],
        ),
    ).drop(min_import_date)
    return df


def impute_missing_registration_dates(df: DataFrame) -> DataFrame:
    """
    Fills missing dates in the imputed_registration_date_column.

    This function fills missing dates in the imputed_registration_date_column. Where there is a value in
    imputed_registation_date for that location at a later point in time, the minimum registration date for
    that location is used. Where there is no value for that location at any point in time, the minimum
    import date is used.

    Args:
        df (DataFrame): A dataframe with the columns imputed_registration_date, location_id, and import_date.

    Returns:
        DataFrame: A dataframe with the imputed_registration_date filled.
    """
    df = df.withColumn(
        CQCLClean.imputed_registration_date,
        F.when(
            df[CQCLClean.imputed_registration_date].isNull(),
            F.min(CQCLClean.imputed_registration_date).over(
                Window.partitionBy(CQCL.location_id)
            ),
        ).otherwise(df[CQCLClean.imputed_registration_date]),
    )
    df = df.withColumn(
        CQCLClean.imputed_registration_date,
        F.when(
            df[CQCLClean.imputed_registration_date].isNull(),
            F.regexp_replace(
                F.min(Keys.import_date).over(Window.partitionBy(CQCL.location_id)),
                "(\d{4})(\d{2})(\d{2})",
                "$1-$2-$3",
            ),
        ).otherwise(df[CQCLClean.imputed_registration_date]),
    )
    return df


# Converted to polars -> Used polars functions flatten_struct_fields and impute_missing_values instead.
def impute_historic_relationships(df: DataFrame) -> DataFrame:
    """
    Imputes historic relationships for locations in the given DataFrame.

    If a location is 'Deregistered' it can have both Predecessors (a previous linked location) and
    Successors (the location it was linked to after closing). A 'Registered' location can only have
    Predecessors. As we are backdating data, locations which were Deregistered when the 'relationship'
    data was first added will need any references to Successors removing when they were previously
    Registered. In order to do this, the function performs the following steps:
    1. Creates a window specification to partition by location_id and order by cqc_location_import_date.
    2. Adds a column 'first_known_relationships' with the first non-null relationship for each location.
    3. Filters the relationships to include only those of type 'HSCA Predecessor'.
    4. Adds a column 'imputed_relationships' based on the following conditions:
       - If 'relationships' is not null, use 'relationships'.
       - If 'registration_status' is 'deregistered', use 'first_known_relationships'.
       - If 'registration_status' is 'registered', use 'relationships_predecessors_only'.
    5. Drops the intermediate columns 'first_known_relationships' and 'relationships_predecessors_only'.

    Args:
        df (DataFrame): Input DataFrame containing location data and relationships.

    Returns:
        DataFrame: DataFrame with imputed historic relationships.
    """
    w = (
        Window.partitionBy(CQCL.location_id)
        .orderBy(CQCLClean.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    df = df.withColumn(
        CQCLClean.first_known_relationships,
        F.first(CQCL.relationships, ignorenulls=True).over(w),
    )
    df = get_relationships_where_type_is_predecessor(df)

    df = df.withColumn(
        CQCLClean.imputed_relationships,
        F.when(F.col(CQCL.relationships).isNotNull(), F.col(CQCL.relationships))
        .when(
            (F.col(CQCL.registration_status) == RegistrationStatus.deregistered),
            F.col(CQCLClean.first_known_relationships),
        )
        .when(
            (F.col(CQCL.registration_status) == RegistrationStatus.registered),
            F.col(CQCLClean.relationships_predecessors_only),
        ),
    )
    df = df.drop(
        CQCLClean.first_known_relationships,
        CQCLClean.relationships_predecessors_only,
    )

    return df


# Converted to polars -> Used polars functions flatten_struct_fields and impute_missing_values instead.
def get_relationships_where_type_is_predecessor(df: DataFrame) -> DataFrame:
    """
    Filters and aggregates relationships of type 'HSCA Predecessor' for each location.

    This function performs the following steps:
    1. Selects distinct location_id and first_known_relationships columns.
    2. Explodes the first_known_relationships column to create a row for each relationship.
    3. Filters the exploded relationships to include only those of type 'HSCA Predecessor'.
    4. Groups by location_id and collects the set of 'HSCA Predecessor' relationships.
    5. Joins the original DataFrame with the aggregated DataFrame on location_id.

    Args:
        df (DataFrame): Input DataFrame containing location data and relationships.

    Returns:
        DataFrame: DataFrame with an additional column for 'HSCA Predecessor' relationships.
    """
    distinct_df = df.select(
        CQCL.location_id, CQCLClean.first_known_relationships
    ).distinct()

    exploded_df = distinct_df.withColumn(
        CQCLClean.relationships_exploded,
        F.explode(CQCLClean.first_known_relationships),
    )
    predecessors_df = exploded_df.filter(
        F.col(f"{CQCLClean.relationships_exploded}.{CQCL.type}") == "HSCA Predecessor"
    )
    aggregated_predecessors_df = predecessors_df.groupby(CQCL.location_id).agg(
        F.collect_set(CQCLClean.relationships_exploded).alias(
            CQCLClean.relationships_predecessors_only
        )
    )

    df = df.join(aggregated_predecessors_df, CQCL.location_id, "left")

    return df


# Converted to polars -> projects._01_ingest.cqc_api.fargate.utils.flatten_utils.impute_missing_struct_columns
def impute_missing_struct_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Imputes missing rows in a struct col in the DataFrame by filling with known values from other import_dates.

    The function performs the following steps:
    1. Creates a new column with the prefix 'imputed_' which copies the column if it contains data,
       and sets it to None if the array is empty.
    2. Fills the missing values in 'imputed_' with the previous known value within the partition.
    3. Fills any remaining missing values in 'imputed_' with the future known value within the partition.

    Args:
        df (DataFrame): Input DataFrame containing 'location_id', 'cqc_location_import_date', and a struct column to impute.
        column_name (str): Name of struct column to impute

    Returns:
        DataFrame: DataFrame with the struct column containing imputed values.
    """
    new_column_name = "imputed_" + column_name
    w_future = Window.partitionBy(CQCL.location_id).orderBy(
        CQCLClean.cqc_location_import_date
    )
    w_historic = w_future.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    df = df.withColumn(
        new_column_name,
        F.when(
            F.size(F.col(column_name)) > 0,
            F.col(column_name),
        ).otherwise(F.lit(None)),
    )

    df = df.withColumn(
        new_column_name,
        F.coalesce(
            F.col(new_column_name),
            F.last(new_column_name, ignorenulls=True).over(w_future),
        ),
    )

    df = df.withColumn(
        new_column_name,
        F.coalesce(
            F.col(new_column_name),
            F.first(new_column_name, ignorenulls=True).over(w_historic),
        ),
    )

    return df


def remove_locations_that_never_had_regulated_activities(
    cqc_df: DataFrame, regulated_activities_dimension: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Removes locations who have never submitted regulated activities data.

    Removes locations who have never submitted regulated activities data as it is
    the activities that are regulated and not the location. This function removes any blank rows
    from the imputed_regulatedactivities column, which are locations that have no data at any time point.

    Args:
        cqc_df (DataFrame): A dataframe without imputed_regulatedactivities, but where the location_ids need to be aligned
        regulated_activities_dimension (DataFrame): A dataframe with imputed_regulatedactivities

    Returns:
        tuple[DataFrame, DataFrame]: cqc_df, regulated_activities_dimension with the rows removed
    """
    to_remove = regulated_activities_dimension.where(
        regulated_activities_dimension[CQCLClean.imputed_regulated_activities].isNull()
    ).select(CQCLClean.location_id)

    # Filter registered location df to remove those not in the regulated_activity_delta
    cqc_df = cqc_df.join(
        to_remove,
        on=CQCLClean.location_id,
        how="left_anti",
    )
    regulated_activities_dimension = regulated_activities_dimension.join(
        to_remove,
        on=CQCLClean.location_id,
        how="left_anti",
    )
    return cqc_df, regulated_activities_dimension


def extract_from_struct(
    df: DataFrame, source_struct_column_name: str, new_column_name: str
) -> DataFrame:
    """
    Extracts data from a specified struct column and stores it in a new column as an array.

    Args:
        df (DataFrame): The input DataFrame.
        source_struct_column_name (str): The name of the column to extract data from.
        new_column_name (str): The name of the new column where extracted data will be stored.

    Returns:
        DataFrame: A new DataFrame with the extracted data stored in the specified column.
    """
    df = df.withColumn(new_column_name, source_struct_column_name)
    return df


def allocate_primary_service_type(df: DataFrame) -> DataFrame:
    """
    Allocates the primary service type for each row in the DataFrame based on the descriptions in the 'imputed_gac_service_types' field.

    primary_service_type is allocated in the following order:
    1) Firstly identify all locations who offer "Care home service with nursing"
    2) Of those who don't, identify all locations who offer "Care home service without nursing"
    3) All other locations are identified as being non residential

    Args:
        df (DataFrame): The input DataFrame containing the 'imputed_gac_service_types' column.

    Returns:
        DataFrame: The DataFrame with the new 'primary_service_type' column added.
    """
    df = df.withColumn(
        CQCLClean.primary_service_type,
        F.when(
            F.array_contains(
                df[CQCLClean.imputed_gac_service_types][CQCL.description],
                "Care home service with nursing",
            ),
            PrimaryServiceType.care_home_with_nursing,
        )
        .when(
            F.array_contains(
                df[CQCLClean.imputed_gac_service_types][CQCL.description],
                "Care home service without nursing",
            ),
            PrimaryServiceType.care_home_only,
        )
        .otherwise(PrimaryServiceType.non_residential),
    )
    return df


def realign_carehome_column_with_primary_service(df: DataFrame) -> DataFrame:
    """
    Allocates the location as a care_home if primary_service_type is a care home.

    Following some missing values in CQC data, the services were imputed. The care_home column is automatically given
    'N' if service is missing. Therefore if imputed service values implied the location was a care home then the
    care_home column needs assigning 'Y'.

    Args:
        df (DataFrame): The input DataFrame containing the 'primary_service_type' column.

    Returns:
        DataFrame: The DataFrame with the 'care_home' column realigned with the 'primary_service_type' column.
    """
    df = df.withColumn(
        CQCLClean.care_home,
        F.when(
            (
                F.col(CQCLClean.primary_service_type)
                == PrimaryServiceType.care_home_with_nursing
            )
            | (
                F.col(CQCLClean.primary_service_type)
                == PrimaryServiceType.care_home_only
            ),
            CareHome.care_home,
        ).otherwise(CareHome.not_care_home),
    )
    return df


def add_related_location_column(df: DataFrame) -> DataFrame:
    """
    Adds a column which flags whether the location was related to a previous location or not

    Args:
        df(DataFrame): A dataframe with the imputed_relationships column.

    Returns:
        DataFrame: A dataframe with a column stating whether the location was related to a previous location or not.
    """
    df = df.withColumn(
        CQCLClean.related_location,
        F.when(
            F.size(F.col(CQCLClean.imputed_relationships)) > 0,
            F.lit(RelatedLocation.has_related_location),
        ).otherwise(
            F.lit(RelatedLocation.no_related_location),
        ),
    )
    return df


def add_cqc_sector_column_to_cqc_locations_dataframe(
    cqc_location_df: DataFrame, la_providerids: list
):
    cqc_location_with_sector_column = cqc_location_df.join(
        create_dataframe_from_la_cqc_location_list(la_providerids),
        CQCL.provider_id,
        "left",
    )

    cqc_location_with_sector_column = cqc_location_with_sector_column.fillna(
        Sector.independent, subset=CQCLClean.cqc_sector
    )

    return cqc_location_with_sector_column


def create_dataframe_from_la_cqc_location_list(la_providerids: list) -> DataFrame:
    spark = utils.get_spark()

    la_locations_dataframe = spark.createDataFrame(
        la_providerids, StringType()
    ).withColumnRenamed("value", CQCL.provider_id)

    la_providers_dataframe = la_locations_dataframe.withColumn(
        CQCLClean.cqc_sector, F.lit(Sector.local_authority).cast(StringType())
    )

    return la_providers_dataframe


if __name__ == "__main__":
    print("Spark job 'clean_cqc_location_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_source,
        cleaned_ons_postcode_directory_source,
        manual_postcode_corrections_source,
        cleaned_cqc_location_destination,
        gac_service_dest,
        regulated_activities_dest,
        specialisms_dest,
        postcode_matching_dest,
    ) = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--cleaned_ons_postcode_directory_source",
            "Source s3 directory for parquet ONS postcode directory dataset",
        ),
        (
            "--manual_postcode_corrections_source",
            "Source s3 location for incorrect postcode csv dataset",
        ),
        (
            "--cleaned_cqc_location_destination",
            "Destination s3 directory for cleaned parquet CQC locations dataset",
        ),
        (
            "--gac_service_destination",
            "Destination s3 directory for GAC service dimension",
        ),
        (
            "--regulated_activities_destination",
            "Destination s3 directory for regulated activities dimension",
        ),
        (
            "--specialisms_destination",
            "Destination s3 directory for specialisms dimension",
        ),
        (
            "--postcode_matching_destination",
            "Destination s3 directory for postcode matching dimension",
        ),
    )
    main(
        cqc_location_source,
        cleaned_ons_postcode_directory_source,
        manual_postcode_corrections_source,
        cleaned_cqc_location_destination,
        gac_service_dest,
        regulated_activities_dest,
        specialisms_dest,
        postcode_matching_dest,
    )

    print("Spark job 'clean_cqc_location_data' complete")
