import sys
import warnings
from typing import Tuple

from utils import utils
import utils.cleaning_utils as cUtils

from pyspark.sql import DataFrame, Window

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
    contemporary_geography_columns,
    current_geography_columns,
)
from utils.cqc_location_dictionaries import InvalidPostcodes


cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cqc_location_api_cols_to_import = [
    CQCL.care_home,
    CQCL.dormancy,
    CQCL.gac_service_types,
    CQCL.location_id,
    CQCL.provider_id,
    CQCL.name,
    CQCL.number_of_beds,
    CQCL.postcode,
    CQCL.registration_date,
    CQCL.registration_status,
    CQCL.regulated_activities,
    CQCL.specialisms,
    CQCL.type,
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
    deregistered_cqc_location_destination: str,
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

    cqc_location_df = remove_non_social_care_locations(cqc_location_df)
    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=CQCLClean.registration_date,  # This will format both registration date and deregistration date
        raw_date_format="yyyy-MM-dd",
    )
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    (
        registered_locations_df,
        deregistered_locations_df,
    ) = split_dataframe_into_registered_and_deregistered_rows(cqc_location_df)

    registered_locations_df = add_list_of_services_offered(registered_locations_df)
    registered_locations_df = allocate_primary_service_type(registered_locations_df)

    registered_locations_df = join_cqc_provider_data(
        registered_locations_df, cqc_provider_df
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

    deduped_deregistered_locations_df = (
        only_keep_first_instance_of_deregistered_locations(deregistered_locations_df)
    )

    utils.write_to_parquet(
        deduped_deregistered_locations_df,
        deregistered_cqc_location_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


def remove_non_social_care_locations(df: DataFrame) -> DataFrame:
    return df.where(df[CQCL.type] == CQCLValues.social_care_identifier)


def join_ons_postcode_data_into_cqc_df(
    cqc_df: DataFrame, ons_df: DataFrame
) -> DataFrame:
    cqc_df = amend_invalid_postcodes(cqc_df)

    cqc_df = utils.normalise_column_values(cqc_df, CQCL.postcode)

    cqc_df = cUtils.add_aligned_date_column(
        cqc_df,
        ons_df,
        CQCLClean.cqc_location_import_date,
        ONSClean.contemporary_ons_import_date,
    )
    ons_df = ons_df.withColumnRenamed(ONSClean.postcode, CQCLClean.postcode)

    return cqc_df.join(
        ons_df,
        [ONSClean.contemporary_ons_import_date, CQCLClean.postcode],
        "left",
    )


def amend_invalid_postcodes(df: DataFrame) -> DataFrame:
    post_codes_mapping = InvalidPostcodes.invalid_postcodes_map

    map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    return df.withColumn(CQCL.postcode, map_func(F.col(CQCL.postcode)))


def add_list_of_services_offered(cqc_loc_df: DataFrame) -> DataFrame:
    cqc_loc_df = cqc_loc_df.withColumn(
        CQCLClean.services_offered, cqc_loc_df[CQCL.gac_service_types].description
    )
    return cqc_loc_df


def allocate_primary_service_type(df: DataFrame):
    return df.withColumn(
        CQCLClean.primary_service_type,
        F.when(
            F.array_contains(
                df[CQCL.gac_service_types].description,
                "Care home service with nursing",
            ),
            CQCLValues.care_home_with_nursing,
        )
        .when(
            F.array_contains(
                df[CQCL.gac_service_types].description,
                "Care home service without nursing",
            ),
            CQCLValues.care_home_only,
        )
        .otherwise(CQCLValues.non_residential),
    )


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


def split_dataframe_into_registered_and_deregistered_rows(
    locations_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    invalid_rows = locations_df.where(
        (locations_df[CQCL.registration_status] != CQCLValues.registered)
        & (locations_df[CQCL.registration_status] != CQCLValues.deregistered)
    ).count()

    if invalid_rows != 0:
        warnings.warn(
            f"{invalid_rows} row(s) has/have an invalid registration status and have been dropped."
        )

    registered_df = locations_df.where(
        locations_df[CQCL.registration_status] == CQCLValues.registered
    )
    deregistered_df = locations_df.where(
        locations_df[CQCL.registration_status] == CQCLValues.deregistered
    )

    return registered_df, deregistered_df


def only_keep_first_instance_of_deregistered_locations(
    df_with_duplicates: DataFrame,
) -> DataFrame:
    # TODO write test
    row_number: str = "row_number"
    window = Window.partitionBy(CQCLClean.location_id).orderBy(
        CQCLClean.cqc_location_import_date
    )
    df_without_duplicates = (
        df_with_duplicates.withColumn(row_number, F.row_number().over(window))
        .filter(F.col(row_number) == 1)
        .drop(F.col(row_number))
    )
    return df_without_duplicates


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
        AnalysisException: If column_to_check_for_nulls, CQCLClean.postcode or CQCLClean.location_id is mistyped or otherwise not present in cleaned_locations_df
        TypeError: If sample_clean_null_df is found not to be empty, will cause a glue job failure where the unmatched postcodes and corresponding locationid should feature in Glue's logs
    """

    COLUMNS_TO_FILTER = [
        CQCLClean.postcode,
        CQCLClean.location_id,
    ]
    list_of_columns = cleaned_locations_df.columns
    for column in [column_to_check_for_nulls, *COLUMNS_TO_FILTER]:
        if not column in list_of_columns:
            raise AnalysisException(
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
            f"Error: The following {CQCL.postcode}(s) and their corresponding {CQCL.location_id}(s) were not found in the ONS postcode data: {list_of_tuples}"
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
        deregistered_cqc_location_destination,
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
        (
            "--deregistered_cqc_location_destination",
            "Destination s3 directory for deregistered CQC locations dataset",
        ),
    )
    main(
        cqc_location_source,
        cleaned_cqc_provider_source,
        cleaned_ons_postcode_directory_source,
        cleaned_cqc_location_destination,
        deregistered_cqc_location_destination,
    )

    print("Spark job 'clean_cqc_location_data' complete")
