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
    CqcLocationApiColumns,
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

    cqc_location_df = allocate_primary_service_type(cqc_location_df)

    utils.write_to_parquet(
        cqc_location_df,
        cleaned_cqc_location_destintion,
        append=True,
        partitionKeys=cqcPartitionKeys,
    )


def allocate_primary_service_type(df: DataFrame):
    return df.withColumn(
        CleanedColumns.primary_service_type,
        F.when(
            F.array_contains(
                df[CqcLocationApiColumns.gac_service_types].description,
                "Care home service with nursing",
            ),
            NURSING_HOME_IDENTIFIER,
        )
        .when(
            F.array_contains(
                df[CqcLocationApiColumns.gac_service_types].description,
                "Care home service without nursing",
            ),
            NONE_NURSING_HOME_IDENTIFIER,
        )
        .otherwise(NONE_RESIDENTIAL_IDENTIFIER),
    )


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
