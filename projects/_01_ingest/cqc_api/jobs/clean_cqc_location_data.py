import sys
import warnings

from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import (
    CareHome,
    Dormancy,
    LocationType,
    PrimaryServiceType,
    RegistrationStatus,
    RelatedLocation,
    Services,
    Specialisms,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.cqc_location_utils.extract_registered_manager_names import (
    extract_registered_manager_names_from_imputed_regulated_activities_column,
)
from utils.raw_data_adjustments import remove_records_from_locations_data
from projects._01_ingest.cqc_api.utils.postcode_matcher import run_postcode_matching

from projects._01_ingest.cqc_api.utils.utils import (
    classify_specialisms,
)

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
cqc_provider_cols_to_import = [
    CQCPClean.provider_id,
    CQCPClean.name,
    CQCPClean.cqc_sector,
    CQCPClean.cqc_provider_import_date,
]


def main(
    cqc_location_source: str,
    cleaned_cqc_provider_source: str,
    cleaned_ons_postcode_directory_source: str,
    cleaned_cqc_location_destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cqc_location_source, selected_columns=cqc_location_api_cols_to_import
    )
    cqc_provider_df = utils.read_from_parquet(
        cleaned_cqc_provider_source, selected_columns=cqc_provider_cols_to_import
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
    cqc_location_df = calculate_time_registered_for(cqc_location_df)
    cqc_location_df = calculate_time_since_dormant(cqc_location_df)

    cqc_location_df = impute_historic_relationships(cqc_location_df)
    registered_locations_df = select_registered_locations_only(cqc_location_df)

    registered_locations_df = impute_missing_struct_column(
        registered_locations_df, CQCL.gac_service_types
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
        IndCQC.specialist_generalist_other_dementia,
        Specialisms.dementia,
    )
    registered_locations_df = classify_specialisms(
        registered_locations_df,
        IndCQC.specialist_generalist_other_lda,
        Specialisms.learning_disabilities,
    )
    registered_locations_df = classify_specialisms(
        registered_locations_df,
        IndCQC.specialist_generalist_other_mh,
        Specialisms.mental_health,
    )
    registered_locations_df = remove_specialist_colleges(registered_locations_df)
    registered_locations_df = allocate_primary_service_type(registered_locations_df)
    registered_locations_df = realign_carehome_column_with_primary_service(
        registered_locations_df
    )
    registered_locations_df = (
        extract_registered_manager_names_from_imputed_regulated_activities_column(
            registered_locations_df
        )
    )

    registered_locations_df = add_related_location_column(registered_locations_df)

    registered_locations_df = join_cqc_provider_data(
        registered_locations_df, cqc_provider_df
    )

    registered_locations_df = impute_missing_data_from_provider_dataset(
        registered_locations_df, CQCLClean.provider_name
    )
    registered_locations_df = impute_missing_data_from_provider_dataset(
        registered_locations_df, CQCLClean.cqc_sector
    )

    registered_locations_df = run_postcode_matching(
        registered_locations_df, ons_postcode_directory_df
    )

    utils.write_to_parquet(
        registered_locations_df,
        cleaned_cqc_location_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


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


def calculate_time_registered_for(df: DataFrame) -> DataFrame:
    """
    Adds a new column called time_registered which is the number of months the location has been registered with CQC for (rounded up).

    This function adds a new integer column to the given data frame which represents the number of months (rounded up) between the
    imputed registration date and the cqc location import date.

    Args:
        df (DataFrame): A dataframe containing the columns: imputed_registration_date and cqc_location_import_date.

    Returns:
        DataFrame: A dataframe with the new time_registered column added.
    """
    df = df.withColumn(
        CQCLClean.time_registered,
        F.floor(
            F.months_between(
                F.col(CQCLClean.cqc_location_import_date),
                F.col(CQCLClean.imputed_registration_date),
            )
        )
        + 1,
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


def impute_missing_data_from_provider_dataset(
    locations_df: DataFrame, column_name: str
) -> DataFrame:
    w = (
        Window.partitionBy(CQCL.provider_id)
        .orderBy(CQCLClean.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    locations_df = locations_df.withColumn(
        column_name,
        F.when(
            locations_df[column_name].isNull(),
            F.first(column_name, ignorenulls=True).over(w),
        ).otherwise(locations_df[column_name]),
    )
    return locations_df


def join_cqc_provider_data(locations_df: DataFrame, provider_df: DataFrame):
    locations_df = cUtils.add_aligned_date_column(
        locations_df,
        provider_df,
        CQCLClean.cqc_location_import_date,
        CQCPClean.cqc_provider_import_date,
    )

    provider_data_to_join_df = provider_df.withColumnRenamed(
        CQCPClean.provider_id, CQCLClean.provider_id
    )
    provider_data_to_join_df = provider_data_to_join_df.withColumnRenamed(
        CQCPClean.name, CQCLClean.provider_name
    )

    joined_df = locations_df.join(
        provider_data_to_join_df,
        [CQCLClean.provider_id, CQCPClean.cqc_provider_import_date],
        how="left",
    )
    return joined_df


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


def calculate_time_since_dormant(df: DataFrame) -> DataFrame:
    """
    Adds a column to show the number of months since the location was last dormant.

    This function calculates the number of months since the last time a location was marked as dormant.
    It uses a window function to track the most recent date when dormancy was marked as "Y" and calculates
    the number of months since that date for each location.

    'time_since_dormant' values before the first instance of dormancy are null.
    If the location has never been dormant then 'time_since_dormant' is null.

    Args:
        df (DataFrame): A dataframe with columns: cqc_location_import_date, dormancy, and location_id.

    Returns:
        DataFrame: A dataframe with an additional column 'time_since_dormant'.
    """
    w = Window.partitionBy(CQCLClean.location_id).orderBy(
        CQCLClean.cqc_location_import_date
    )

    df = df.withColumn(
        CQCLClean.dormant_date,
        F.when(
            F.col(CQCLClean.dormancy) == Dormancy.dormant,
            F.col(CQCLClean.cqc_location_import_date),
        ),
    )

    df = df.withColumn(
        CQCLClean.last_dormant_date,
        F.last(CQCLClean.dormant_date, ignorenulls=True).over(w),
    )

    df = df.withColumn(
        CQCLClean.time_since_dormant,
        F.when(
            F.col(CQCLClean.last_dormant_date).isNotNull(),
            F.when(F.col(CQCLClean.dormancy) == Dormancy.dormant, 1).otherwise(
                F.floor(
                    F.months_between(
                        F.col(CQCLClean.cqc_location_import_date),
                        F.col(CQCLClean.last_dormant_date),
                    )
                )
                + 1,
            ),
        ),
    )

    df = df.drop(
        CQCLClean.dormant_date,
        CQCLClean.last_dormant_date,
    )

    return df


if __name__ == "__main__":
    print("Spark job 'clean_cqc_location_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_source,
        cleaned_cqc_provider_source,
        cleaned_ons_postcode_directory_source,
        cleaned_cqc_location_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--cleaned_cqc_provider_source",
            "Source s3 directory for cleaned parquet CQC provider dataset",
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
        cleaned_cqc_provider_source,
        cleaned_ons_postcode_directory_source,
        cleaned_cqc_location_destination,
    )

    print("Spark job 'clean_cqc_location_data' complete")
