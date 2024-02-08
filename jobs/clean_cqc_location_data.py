import sys

from utils import utils

from pyspark.sql.dataframe import DataFrame

import pyspark.sql.functions as F

from utils.column_names.cleaned_data_files.cqc_location_data_columns import (
    CqcLocationCleanedColumns as CleanedColumns,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_provider_data_columns_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_data_columns import (
    CqcLocationCleanedColumns as CQCLClean,
)

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

NURSING_HOME_IDENTIFIER = "Care home with nursing"
NONE_NURSING_HOME_IDENTIFIER = "Care home without nursing"
NONE_RESIDENTIAL_IDENTIFIER = "non-residential"


def main(
    cqc_location_source: str,
    cleaned_provider_source: str,
    cleaned_cqc_location_destintion: str,
):
    cqc_location_df = utils.read_from_parquet(cqc_location_source)
    cqc_provider_df = utils.read_from_parquet(cleaned_provider_source)

    cqc_location_df = join_cqc_provider_data(cqc_location_df, cqc_provider_df)

    cqc_location_df = allocate_primary_service_type(cqc_location_df)

    registered_locations_df, deregistered_locations_df = split_dataframe_into_registered_and_deregistered_rows(cqc_location_df)

    utils.write_to_parquet(
        registered_locations_df,
        cleaned_cqc_location_destintion,
        append=True,
        partitionKeys=cqcPartitionKeys,
    )


def allocate_primary_service_type(df: DataFrame):
    return df.withColumn(
        CleanedColumns.primary_service_type,
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

def split_dataframe_into_registered_and_deregistered_rows(locations_df:DataFrame) -> DataFrame:
    registered_df = locations_df.where(locations_df[CQCL.registration_status] == "Registered")
    deregistered_df = locations_df.where(locations_df[CQCL.registration_status] == "Deregistered")
    rows_without_registration_status = locations_df.where(locations_df[CQCL.registration_status].isNull()).count()
    if rows_without_registration_status != 0:
        print(f"{rows_without_registration_status} rows are missing a registration status and have been dropped.")
    return registered_df, deregistered_df

if __name__ == "__main__":
    print("Spark job 'clean_cqc_location_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_source,
        cleaned_provider_source,
        cleaned_cqc_location_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--cqc_provider_cleaned",
            "Source s3 directory for cleaned parquet CQC provider dataset",
        ),
        (
            "--cleaned_cqc_location_destination",
            "Destination s3 directory for cleaned parquet CQC locations dataset",
        ),
    )
    main(cqc_location_source, cleaned_provider_source, cleaned_cqc_location_destination)

    print("Spark job 'clean_cqc_location_data' complete")
