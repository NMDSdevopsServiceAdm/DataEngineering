import sys

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StringType,
)

from utils import utils
import utils.cleaning_utils as cUtils

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.cqc_local_authority_provider_ids import LocalAuthorityProviderIds
from utils.column_names.raw_data_files.cqc_provider_api_columns import (
    CqcProviderApiColumns as CQCP,
)
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_values.categorical_column_values import (
    Sector,
)


cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

known_la_providerids = LocalAuthorityProviderIds.known_ids


def main(cqc_source: str, cleaned_cqc_destination: str):
    cqc_provider_df = utils.read_from_parquet(cqc_source)

    cqc_provider_df = cUtils.column_to_date(
        cqc_provider_df, Keys.import_date, CQCPClean.cqc_provider_import_date
    )

    cqc_provider_df = add_cqc_sector_column_to_cqc_provider_dataframe(
        cqc_provider_df, known_la_providerids
    )

    utils.write_to_parquet(
        cqc_provider_df,
        cleaned_cqc_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


def create_dataframe_from_la_cqc_provider_list(la_providerids: list) -> DataFrame:
    spark = utils.get_spark()

    la_providers_dataframe = spark.createDataFrame(
        la_providerids, StringType()
    ).withColumnRenamed("value", CQCP.provider_id)

    la_providers_dataframe = la_providers_dataframe.withColumn(
        CQCPClean.cqc_sector, F.lit(Sector.local_authority).cast(StringType())
    )

    return la_providers_dataframe


def add_cqc_sector_column_to_cqc_provider_dataframe(
    cqc_provider_df: DataFrame, la_providerids: list
):
    cqc_provider_with_sector_column = cqc_provider_df.join(
        create_dataframe_from_la_cqc_provider_list(la_providerids),
        CQCP.provider_id,
        "left",
    )

    cqc_provider_with_sector_column = cqc_provider_with_sector_column.fillna(
        Sector.independent, subset=CQCPClean.cqc_sector
    )

    return cqc_provider_with_sector_column


if __name__ == "__main__":
    print("Spark job 'clean_cqc_provider_data' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--cqc_provider_source",
            "Source s3 directory for parquet CQC providers dataset",
        ),
        (
            "--cqc_provider_cleaned",
            "Destination s3 directory for cleaned parquet CQC providers dataset",
        ),
    )
    main(source, destination)

    print("Spark job 'clean_cqc_provider_data' complete")
