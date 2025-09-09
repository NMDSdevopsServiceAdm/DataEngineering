import os
import sys
import warnings

os.environ["SPARK_VERSION"] = "3.5"

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
    PartitionKeys as Keys,
    DimensionPartitionKeys as DimensionKeys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    LocationType,
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
    cleaned_cqc_location_destination: str,
    gac_service_destination: str,
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

    known_la_providerids = LocalAuthorityProviderIds.known_ids
    cqc_location_df = add_cqc_sector_column_to_cqc_locations_dataframe(
        cqc_location_df, known_la_providerids
    )

    cqc_location_df = remove_non_social_care_locations(cqc_location_df)
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
    registered_locations_df = select_registered_locations_only(cqc_location_df)

    dimension_update_date = registered_locations_df.agg(
        F.max(Keys.import_date)
    ).collect()[0][0]

    # Create GAC Service dimension
    gac_service_delta = create_dimension_from_missing_struct_column(
        registered_locations_df,
        CQCL.gac_service_types,
        gac_service_destination,
        dimension_update_date,
    )

    registered_locations_df = impute_missing_struct_column(
        registered_locations_df, CQCL.regulated_activities
    )
    registered_locations_df = impute_missing_struct_column(
        registered_locations_df, CQCL.specialisms
    )
    registered_locations_df = remove_locations_that_never_had_regulated_activities(
        registered_locations_df
    )
    registered_locations_df = extract_from_struct(
        registered_locations_df,
        registered_locations_df[CQCLClean.imputed_gac_service_types][CQCL.description],
        CQCLClean.services_offered,
    )
    registered_locations_df = extract_from_struct(
        registered_locations_df,
        registered_locations_df[CQCLClean.imputed_specialisms][CQCL.name],
        CQCLClean.specialisms_offered,
    )
    registered_locations_df = classify_specialisms(
        registered_locations_df,
        Specialisms.dementia,
    )
    registered_locations_df = classify_specialisms(
        registered_locations_df,
        Specialisms.learning_disabilities,
    )
    registered_locations_df = classify_specialisms(
        registered_locations_df,
        Specialisms.mental_health,
    )
    registered_locations_df = remove_specialist_colleges(registered_locations_df)
    registered_locations_df = allocate_primary_service_type(registered_locations_df)
    registered_locations_df = realign_carehome_column_with_primary_service(
        registered_locations_df
    )
    registered_locations_df = extract_registered_manager_names(registered_locations_df)

    registered_locations_df = add_related_location_column(registered_locations_df)

    registered_locations_df = run_postcode_matching(
        registered_locations_df, ons_postcode_directory_df
    )

    utils.write_to_parquet(
        registered_locations_df,
        cleaned_cqc_location_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


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
    previous_dimension = utils.read_from_parquet(dimension_location)

    current_dimension = impute_missing_struct_column(
        df.select(
            CQCL.location_id,
            missing_struct_column,
            Keys.import_date,
        ),
        missing_struct_column,
    ).select(
        CQCLClean.location_id,
        missing_struct_column,
        "imputed_" + missing_struct_column,
        Keys.import_date,
    )
    gac_service_delta = current_dimension.join(
        previous_dimension,
        on=[
            CQCLClean.location_id,
            missing_struct_column,
            "imputed_" + missing_struct_column,
            Keys.import_date,
        ],
        how="anti",
    ).withColumn(DimensionKeys.last_updated, F.lit(dimension_update_date))

    return gac_service_delta


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


def remove_non_social_care_locations(df: DataFrame) -> DataFrame:
    return df.where(df[CQCL.type] == LocationType.social_care_identifier)


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


def remove_locations_that_never_had_regulated_activities(df: DataFrame) -> DataFrame:
    """
    Removes locations who have never submitted regulated activities data.

    Removes locations who have never submitted regulated activities data as it is
    the activities that are regulated and not the location. This function removes any blank rows
    from the imputed_regulatedactivities column, which are locations that have no data at any time point.

    Args:
        df (DataFrame): A dataframe with imputed_regulatedactivities

    Returns:
        DataFrame: A dataframe where blank imputed rows are removed.
    """
    df = df.where(df[CQCLClean.imputed_regulated_activities].isNotNull())
    return df


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


def remove_specialist_colleges(df: DataFrame) -> DataFrame:
    """
    Removes rows where 'Specialist college service' is the only service listed in 'services_offered'.

    We do not include locations which are only specialist colleges in our
    estimates. This function identifies and removes the ones listed in the locations dataset.

    Args:
        df (DataFrame): A cleaned locations dataframe with the services_offered column already created.

    Returns:
        DataFrame: A cleaned locations dataframe with locations which are only specialist colleges removed.
    """
    df = df.where(
        (df[CQCLClean.services_offered][0] != Services.specialist_college_service)
        | (F.size(df[CQCLClean.services_offered]) != 1)
        | (df[CQCLClean.services_offered].isNull())
    )
    return df


def select_registered_locations_only(locations_df: DataFrame) -> DataFrame:
    invalid_rows = locations_df.where(
        (locations_df[CQCL.registration_status] != RegistrationStatus.registered)
        & (locations_df[CQCL.registration_status] != RegistrationStatus.deregistered)
    ).count()

    if invalid_rows != 0:
        warnings.warn(
            f"{invalid_rows} row(s) has/have an invalid registration status and have been dropped."
        )

    locations_df = locations_df.where(
        locations_df[CQCL.registration_status] == RegistrationStatus.registered
    )
    return locations_df


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
        cleaned_cqc_location_destination,
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
            "--cleaned_cqc_location_destination",
            "Destination s3 directory for cleaned parquet CQC locations dataset",
        ),
    )
    main(
        cqc_location_source,
        cleaned_ons_postcode_directory_source,
        cleaned_cqc_location_destination,
    )

    print("Spark job 'clean_cqc_location_data' complete")
