import sys

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
)
import pyspark.sql.functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

from utils.cqc_local_authority_provider_ids import LocalAuthorityProviderIds


cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

known_la_providerids = LocalAuthorityProviderIds.known_ids


def main(cqc_source: str, cleaned_cqc_destination: str):
    cqc_provider_df = utils.read_from_parquet(cqc_source)

    cqc_provider_df = add_cqc_sector_column_to_cqc_provider_dataframe(
        cqc_provider_df, known_la_providerids
    )

    utils.write_to_parquet(
        cqc_provider_df,
        cleaned_cqc_destination,
        append=True,
        partitionKeys=cqcPartitionKeys,
    )


def create_dataframe_from_la_cqc_provider_list(la_providerids: list[str]) -> DataFrame:
    # Start a spark session.
    spark = utils.get_spark()

    la_providers_dataframe = spark.createDataFrame(
        la_providerids, StringType()
    ).withColumnRenamed("value", "providerId")

    la_providers_dataframe = la_providers_dataframe.withColumn(
        "cqc_sector", F.lit("Local authority").cast(StringType())
    )

    return la_providers_dataframe


def add_cqc_sector_column_to_cqc_provider_dataframe(
    cqc_provider_df: DataFrame, la_providerids: list[str]
):
    cqc_provider_with_sector_column = cqc_provider_df.join(
        create_dataframe_from_la_cqc_provider_list(la_providerids),
        "providerId",
        "left",
    )

    cqc_provider_with_sector_column = cqc_provider_with_sector_column.fillna(
        "Independent"
    )

    return cqc_provider_with_sector_column


if __name__ == "__main__":
    # Where we tell Glue how to run the file, and what to print out
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
    # Python logic ---> all in main
    main(source, destination)

    print("Spark job 'clean_cqc_provider_data' complete")
