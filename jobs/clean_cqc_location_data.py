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
)
from utils.cqc_location_dictionaries import InvalidPostcodes

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

NURSING_HOME_IDENTIFIER = "Care home with nursing"
NONE_NURSING_HOME_IDENTIFIER = "Care home without nursing"
NONE_RESIDENTIAL_IDENTIFIER = "non-residential"

DATE_COLUMN_IDENTIFIER = "registration_date"


def main(
    cqc_location_source: str,
    cleaned_cqc_provider_source: str,
    ons_postcode_directory_source: str,
    cleaned_cqc_location_destintion: str,
):
    cqc_location_df = utils.read_from_parquet(cqc_location_source)
    cqc_provider_df = utils.read_from_parquet(cleaned_cqc_provider_source)
    ons_postcode_directory_df = utils.read_from_parquet(ons_postcode_directory_source)

    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, Keys.import_date, CQCLClean.cqc_location_import_date
    )

    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=DATE_COLUMN_IDENTIFIER,
        raw_date_format="yyyy-MM-dd",
    )

    cqc_location_df = remove_non_social_care_locations(cqc_location_df)

    cqc_location_df = remove_invalid_postcodes(cqc_location_df)

    cqc_location_df = join_cqc_provider_data(cqc_location_df, cqc_provider_df)

    cqc_location_df = allocate_primary_service_type(cqc_location_df)

    (
        registered_locations_df,
        deregistered_locations_df,
    ) = split_dataframe_into_registered_and_deregistered_rows(cqc_location_df)

    utils.write_to_parquet(
        registered_locations_df,
        cleaned_cqc_location_destintion,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


def remove_non_social_care_locations(df: DataFrame):
    return df.where(df[CQCL.type] == "Social Care Org")


def remove_invalid_postcodes(df: DataFrame):
    post_codes_mapping = InvalidPostcodes.invalid_postcodes_map

    map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    return df.withColumn(CQCL.postcode, map_func(F.col(CQCL.postcode)))


def allocate_primary_service_type(df: DataFrame):
    return df.withColumn(
        CQCLClean.primary_service_type,
        F.when(
            F.array_contains(
                df[CQCL.gac_service_types].description,
                "Care home service with nursing",
            ),
            NURSING_HOME_IDENTIFIER,
        )
        .when(
            F.array_contains(
                df[CQCL.gac_service_types].description,
                "Care home service without nursing",
            ),
            NONE_NURSING_HOME_IDENTIFIER,
        )
        .otherwise(NONE_RESIDENTIAL_IDENTIFIER),
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
        (locations_df[CQCL.registration_status] != "Registered")
        & (locations_df[CQCL.registration_status] != "Deregistered")
    ).count()

    if invalid_rows != 0:
        warnings.warn(
            f"{invalid_rows} row(s) has/have an invalid registration status and have been dropped."
        )

    registered_df = locations_df.where(
        locations_df[CQCL.registration_status] == "Registered"
    )
    deregistered_df = locations_df.where(
        locations_df[CQCL.registration_status] == "Deregistered"
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
        cleaned_cqc_location_destination,
    )

    print("Spark job 'clean_cqc_location_data' complete")
