import sys
import warnings

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
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.cqc_location_dictionaries import InvalidPostcodes

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

DATE_COLUMN_IDENTIFIER = "registration_date"

ons_cols_to_import = [
    ONS.import_date,
    ONS.cssr,
    ONS.region,
    ONS.icb,
    ONS.rural_urban_indicator_2011,
    ONS.postcode,
]

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


def main(
    cqc_location_source: str,
    cleaned_cqc_provider_source: str,
    ons_postcode_directory_source: str,
    cleaned_cqc_location_destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cqc_location_source, selected_columns=cqc_location_api_cols_to_import
    )
    cqc_provider_df = utils.read_from_parquet(cleaned_cqc_provider_source)
    ons_postcode_directory_df = utils.read_from_parquet(
        ons_postcode_directory_source, selected_columns=ons_cols_to_import
    )

    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=DATE_COLUMN_IDENTIFIER,
        raw_date_format="yyyy-MM-dd",
    )

    ons_postcode_directory_df = cUtils.column_to_date(
        ons_postcode_directory_df, Keys.import_date, CQCLClean.ons_import_date
    ).drop(ONS.import_date)

    current_ons_postcode_directory_df = prepare_current_ons_data(
        ons_postcode_directory_df
    )

    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    cqc_location_df = remove_non_social_care_locations(cqc_location_df)

    cqc_location_df = remove_invalid_postcodes(cqc_location_df)

    cqc_location_df = join_contemporary_ons_postcode_data(
        cqc_location_df, ons_postcode_directory_df
    )

    cqc_location_df = utils.normalise_column_values(cqc_location_df, CQCL.postcode)

    cqc_location_df = join_current_ons_postcode_data(
        cqc_location_df, current_ons_postcode_directory_df
    )

    cqc_location_df = join_cqc_provider_data(cqc_location_df, cqc_provider_df)

    cqc_location_df = add_list_of_services_offered(cqc_location_df)

    cqc_location_df = allocate_primary_service_type(cqc_location_df)

    (
        registered_locations_df,
        deregistered_locations_df,
    ) = split_dataframe_into_registered_and_deregistered_rows(cqc_location_df)

    utils.write_to_parquet(
        registered_locations_df,
        cleaned_cqc_location_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


def prepare_current_ons_data(ons_df: DataFrame):
    max_import_date = ons_df.agg(F.max(CQCLClean.ons_import_date)).collect()[0][0]
    current_ons_df = ons_df.filter(F.col(CQCLClean.ons_import_date) == max_import_date)

    STRING_TO_PREPEND = "current_"
    COLS_TO_RENAME = current_ons_df.columns
    COLS_TO_RENAME.remove(ONS.postcode)

    new_ons_col_names = [STRING_TO_PREPEND + col for col in COLS_TO_RENAME]

    for i in range(len(COLS_TO_RENAME)):
        current_ons_df = current_ons_df.withColumnRenamed(
            COLS_TO_RENAME[i], new_ons_col_names[i]
        )

    current_ons_df = current_ons_df.withColumnRenamed(ONS.postcode, CQCL.postcode)

    return current_ons_df


def remove_non_social_care_locations(df: DataFrame):
    return df.where(df[CQCL.type] == CQCLValues.social_care_identifier)


def remove_invalid_postcodes(df: DataFrame):
    post_codes_mapping = InvalidPostcodes.invalid_postcodes_map

    map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    return df.withColumn(CQCL.postcode, map_func(F.col(CQCL.postcode)))


def join_contemporary_ons_postcode_data(
    cqc_location_df: DataFrame, ons_postcode_directory_df: DataFrame
) -> DataFrame:
    cqc_location_df = cUtils.add_aligned_date_column(
        cqc_location_df,
        ons_postcode_directory_df,
        CQCLClean.cqc_location_import_date,
        CQCLClean.ons_import_date,
    )
    formatted_ons_postcode_directory_df = ons_postcode_directory_df.withColumnRenamed(
        ONS.postcode, CQCLClean.postcode
    )

    cqc_location_df = cqc_location_df.join(
        formatted_ons_postcode_directory_df,
        [CQCLClean.ons_import_date, CQCLClean.postcode],
        "left",
    )
    return cqc_location_df


def join_current_ons_postcode_data(cqc_loc_df: DataFrame, current_ons_df: DataFrame):
    return cqc_loc_df.join(
        current_ons_df,
        CQCL.postcode,
        "left",
    )


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
    provider_data_to_join_df = provider_df.select(
        provider_df[CQCPClean.provider_id].alias("provider_id_to_drop"),
        provider_df[CQCPClean.name].alias(CQCLClean.provider_name),
        provider_df[CQCPClean.cqc_sector],
        provider_df[Keys.import_date].alias("import_date_to_drop"),
    )
    columns_to_join = [
        locations_df[CQCL.provider_id]
        == provider_data_to_join_df["provider_id_to_drop"],
        locations_df[Keys.import_date]
        == provider_data_to_join_df["import_date_to_drop"],
    ]
    joined_df = locations_df.join(
        provider_data_to_join_df, columns_to_join, how="left"
    ).drop("provider_id_to_drop", "import_date_to_drop")

    return joined_df


def split_dataframe_into_registered_and_deregistered_rows(
    locations_df: DataFrame,
) -> DataFrame:
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


if __name__ == "__main__":
    print("Spark job 'clean_cqc_location_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_source,
        cleaned_cqc_provider_source,
        ons_postcode_directory_source,
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
            "--ons_postcode_directory_source",
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
        ons_postcode_directory_source,
        cleaned_cqc_location_destination,
    )

    print("Spark job 'clean_cqc_location_data' complete")
