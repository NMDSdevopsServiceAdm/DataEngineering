import sys

import pyspark.sql.dataframe

from utils import utils


def get_cqc_provider_df(cqc_provider_source, spark_session) -> pyspark.sql.DataFrame:
    print(f"Reading CQC providers parquet from {cqc_provider_source}")
    cqc_provider_df = spark_session.read.option(
        "basePath", cqc_provider_source
    ).parquet(  # used for partitioning
        cqc_provider_source
    )

    return cqc_provider_df


def clean_cqc_provider_df(df_to_clean) -> pyspark.sql.DataFrame:
    # Cleaning logic
    return df_to_clean


def write_cleaned_provider_df_to_parquet(dataFrame, destination) -> None:
    utils.write_to_parquet(
        dataFrame,
        destination,
        append=True,
        partitionKeys=["year", "month", "day", "import_date"],
    )


def main(cqc_source: str, cleaned_cqc_destination: str):
    spark = utils.get_spark()
    cqc_provider_df = get_cqc_provider_df(cqc_source, spark)
    cleaned_cqc_provider_df = clean_cqc_provider_df(cqc_provider_df)
    write_cleaned_provider_df_to_parquet(
        cleaned_cqc_provider_df, cleaned_cqc_destination
    )


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
