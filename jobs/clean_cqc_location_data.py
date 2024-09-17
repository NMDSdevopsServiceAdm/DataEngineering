import sys
import warnings

from pyspark.sql import DataFrame, Window, functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    LocationType,
    PrimaryServiceType,
    RegistrationStatus,
    Services,
    RelatedLocation,
)
from utils.cqc_location_dictionaries import InvalidPostcodes
from utils.raw_data_adjustments import remove_records_from_locations_data


cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cqc_location_api_cols_to_import = [
    CQCL.care_home,
    CQCL.dormancy,
    CQCL.gac_service_types,
    CQCL.location_id,
    CQCL.provider_id,
    CQCL.name,
    CQCL.number_of_beds,
    CQCL.postal_code,
    CQCL.registration_status,
    CQCL.registration_date,
    CQCL.deregistration_date,
    CQCL.regulated_activities,
    CQCL.specialisms,
    CQCL.type,
    CQCL.relationships,
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

    registered_locations_df = select_registered_locations_only(cqc_location_df)

    registered_locations_df = impute_missing_gac_service_types(registered_locations_df)
    registered_locations_df = add_list_of_services_offered(registered_locations_df)
    registered_locations_df = remove_specialist_colleges(registered_locations_df)
    registered_locations_df = allocate_primary_service_type(registered_locations_df)
    registered_locations_df = add_column_related_location(registered_locations_df)
    # TODO - recreate carehome column following impute_missing_gac_service_types using primary_service_type column

    registered_locations_df = join_cqc_provider_data(
        registered_locations_df, cqc_provider_df
    )

    registered_locations_df = impute_missing_data_from_provider_dataset(
        registered_locations_df, CQCLClean.provider_name
    )
    registered_locations_df = impute_missing_data_from_provider_dataset(
        registered_locations_df, CQCLClean.cqc_sector
    )

    registered_locations_df = join_ons_postcode_data_into_cqc_df(
        registered_locations_df, ons_postcode_directory_df
    )
    registered_locations_df = raise_error_if_cqc_postcode_was_not_found_in_ons_dataset(
        registered_locations_df
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


def remove_non_social_care_locations(df: DataFrame) -> DataFrame:
    return df.where(df[CQCL.type] == LocationType.social_care_identifier)


def join_ons_postcode_data_into_cqc_df(
    cqc_df: DataFrame, ons_df: DataFrame
) -> DataFrame:
    cqc_df = amend_invalid_postcodes(cqc_df)

    cqc_df = utils.normalise_column_values(cqc_df, CQCL.postal_code)

    cqc_df = cUtils.add_aligned_date_column(
        cqc_df,
        ons_df,
        CQCLClean.cqc_location_import_date,
        ONSClean.contemporary_ons_import_date,
    )
    ons_df = ons_df.withColumnRenamed(ONSClean.postcode, CQCLClean.postal_code)

    cqc_df = cqc_df.join(
        ons_df,
        [ONSClean.contemporary_ons_import_date, CQCLClean.postal_code],
        "left",
    )
    return cqc_df


def amend_invalid_postcodes(df: DataFrame) -> DataFrame:
    post_codes_mapping = InvalidPostcodes.invalid_postcodes_map

    map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    df = df.withColumn(CQCL.postal_code, map_func(F.col(CQCL.postal_code)))
    return df


def impute_missing_gac_service_types(df: DataFrame) -> DataFrame:
    """
    Imputes missing gacservicetypes in the DataFrame by filling with known values from other imports.

    The function performs the following steps:
    1. Creates a new column 'imputed_gac_service_types' which copies gac_service_types if it contains data,
       and sets it to None if the array is empty.
    2. Fills the missing values in 'imputed_gac_service_types' with the previous known value within the partition.
    3. Fills any remaining missing values in 'imputed_gac_service_types' with the future known value within the partition.

    Args:
        df (DataFrame): Input DataFrame containing 'location_id', 'cqc_location_import_date', and 'gac_service_types'.

    Returns:
        DataFrame: DataFrame with the 'imputed_gac_service_types' column containing imputed values.
    """
    w_future = Window.partitionBy(CQCL.location_id).orderBy(
        CQCLClean.cqc_location_import_date
    )
    w_historic = w_future.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    df = df.withColumn(
        CQCLClean.imputed_gac_service_types,
        F.when(
            F.size(F.col(CQCL.gac_service_types)) > 0,
            F.col(CQCL.gac_service_types),
        ).otherwise(F.lit(None)),
    )

    df = df.withColumn(
        CQCLClean.imputed_gac_service_types,
        F.coalesce(
            F.col(CQCLClean.imputed_gac_service_types),
            F.last(CQCLClean.imputed_gac_service_types, ignorenulls=True).over(
                w_future
            ),
        ),
    )

    df = df.withColumn(
        CQCLClean.imputed_gac_service_types,
        F.coalesce(
            F.col(CQCLClean.imputed_gac_service_types),
            F.first(CQCLClean.imputed_gac_service_types, ignorenulls=True).over(
                w_historic
            ),
        ),
    )

    return df


def add_list_of_services_offered(cqc_loc_df: DataFrame) -> DataFrame:
    """
    Adds a new column called 'services_offered' which contains an array of descriptions from the 'imputed_gac_service_types' field.

    Args:
        cqc_loc_df (DataFrame): The input DataFrame containing the 'imputed_gac_service_types' column.

    Returns:
        DataFrame: The DataFrame with the new 'services_offered' column added.
    """
    cqc_loc_df = cqc_loc_df.withColumn(
        CQCLClean.services_offered,
        cqc_loc_df[CQCLClean.imputed_gac_service_types][CQCL.description],
    )
    return cqc_loc_df


def allocate_primary_service_type(df: DataFrame):
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


def add_column_related_location(df: DataFrame) -> DataFrame:
    """
    Adds a column which flags whether the location has a related previous location or not

    Args:
        df(DataFrame): A dataframe with the relationships column.

    Returns:
        DataFrame: A dataframe with a column stating whther the locaiton has a related location or not.
    """
    df = df.withColumn(
        CQCLClean.related_location,
        F.when(
            F.size(df[CQCL.relationships]) > 0,
            F.lit(RelatedLocation.has_related_location),
        ).when(
            F.size(df[CQCL.relationships]) == 0,
            F.lit(RelatedLocation.no_related_location),
        ),
    )
    return df


def remove_specialist_colleges(df: DataFrame):
    """
    Removes rows where 'Specialist college service' is the only service listed in 'services_offered'.

    We do not include locations which are only specialist colleges in our
    estimates. This function identifies and removes the ones listed in the locations dataset.

    Args:
        df (DataFrame): A cleaned locations dataframe with the services_offered column already created.

    Returns:
        (DataFrame): A cleaned locations dataframe with locations which are only specialist colleges removed.
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


def raise_error_if_cqc_postcode_was_not_found_in_ons_dataset(
    cleaned_locations_df: DataFrame,
    column_to_check_for_nulls: str = CQCLClean.current_ons_import_date,
) -> DataFrame:
    """
    Checks a cleaned locations df for any CQC postcodes which could not be found in the ONS postcode directory based on the column_to_check_for_nulls.
    This column should thus be chosen to be one column that would contain null values from a left join in the above case.
    This is because this function attempts to create a small dataframe which should be empty in the case where column_to_check_for_nulls contains no nulls,
    and thus implies all CQC postcodes were found in the ONS postcode directory.

    Args:
        cleaned_locations_df (DataFrame): A cleaned locations df that must contain at least the column_to_check_for_nulls, postcode and locationId.
        column_to_check_for_nulls (str): Default - current_ons_import_date, should be any one of the left-joined columns to check for null entries.

    Returns:
        cleaned_locations_df (DataFrame): If the check does not find any null entries, it returns the original df. If it does find anything exceptions will be raised instead.

    Raises:
        AnalysisException: If column_to_check_for_nulls, CQCLClean.postal_code or CQCLClean.location_id is mistyped or otherwise not present in cleaned_locations_df
        TypeError: If sample_clean_null_df is found not to be empty, will cause a glue job failure where the unmatched postcodes and corresponding locationid should feature in Glue's logs
    """

    COLUMNS_TO_FILTER = [
        CQCLClean.postal_code,
        CQCLClean.location_id,
    ]
    list_of_columns = cleaned_locations_df.columns
    for column in [column_to_check_for_nulls, *COLUMNS_TO_FILTER]:
        if column not in list_of_columns:
            raise ValueError(
                f"ERROR: A column or function parameter with name {column} cannot be found in the dataframe."
            )

    sample_clean_null_df = cleaned_locations_df.select(
        [column_to_check_for_nulls, *COLUMNS_TO_FILTER]
    ).filter(F.col(column_to_check_for_nulls).isNull())
    if not sample_clean_null_df.rdd.isEmpty():
        list_of_tuples = []
        data_to_log = (
            sample_clean_null_df.select(COLUMNS_TO_FILTER)
            .groupBy(COLUMNS_TO_FILTER)
            .count()
            .collect()
        )

        for row in data_to_log:
            list_of_tuples.append((row[0], row[1], f"count: {row[2]}"))
        raise TypeError(
            f"Error: The following {CQCL.postal_code}(s) and their corresponding {CQCL.location_id}(s) were not found in the ONS postcode data: {list_of_tuples}"
        )
    else:
        print(
            "All postcodes were found in the ONS postcode file, returning original dataframe"
        )
        return cleaned_locations_df


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
