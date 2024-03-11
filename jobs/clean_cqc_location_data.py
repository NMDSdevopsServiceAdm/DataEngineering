import sys
import warnings
from typing import Tuple

from utils import utils
import utils.cleaning_utils as cUtils

from pyspark.sql.dataframe import DataFrame

import pyspark.sql.functions as F

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
    cqc_location_df = add_list_of_services_offered(cqc_location_df)
    cqc_location_df = allocate_primary_service_type(cqc_location_df)
    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=CQCLClean.registration_date,  # This will format both registration date and deregistration date
        raw_date_format="yyyy-MM-dd",
    )
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    cqc_location_df = join_ons_postcode_data_into_cqc_df(
        cqc_location_df, ons_postcode_directory_df
    )

    cqc_location_df = join_cqc_provider_data(cqc_location_df, cqc_provider_df)

    (
        registered_locations_df,
        deregistered_locations_df,
    ) = split_dataframe_into_registered_and_deregistered_rows(cqc_location_df)

    registered_locations_df = check_current_against_contemporary_geographies(
        registered_locations_df
    )

    utils.write_to_parquet(
        registered_locations_df,
        cleaned_cqc_location_destination,
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


def check_current_against_contemporary_geographies(
    cleaned_locations_df: DataFrame,
    column_to_check_for_nulls: str = ONSClean.current_ons_import_date,
) -> DataFrame:
    """
    Checks a cleaned locations df for any mismatches of contemporary versus current geographies based on the column_to_check_for_nulls,
    where this column is a subset of columns resulting from a left join and so could contain null values.
    """
    sample_clean_null_df = cleaned_locations_df.filter(
        F.col(column_to_check_for_nulls).isNull()
    )
    if sample_clean_null_df.rdd.isEmpty() == False:
        list_of_tuples = []
        cols_to_return = [
            CQCL.postcode,
            CQCL.location_id,
        ]
        data_to_log = (
            sample_clean_null_df.select(cols_to_return)
            .groupBy(cols_to_return)
            .count()
            .collect()
        )

        for row in data_to_log:
            list_of_tuples.append((row[0], row[1], f"count: {row[2]}"))
        sys.exit(
            f"Error: Problem matching contemporary to current ons data.\nHere is a list of their {CQCL.postcode} and {CQCL.location_id}:\n{list_of_tuples}"
        )
    print("No mismatches found, returning original dataframe")
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
