import sys

import pyspark.sql.dataframe

from utils import utils


def clean_cqc_provider_df(df_to_clean) -> pyspark.sql.DataFrame:
    # Cleaning logic
    return df_to_clean


def main(cqc_source: str, cleaned_cqc_destination: str):
    cqc_provider_df = utils.read_from_parquet(cqc_source)
    print(cqc_provider_df)
    cleaned_cqc_provider_df = clean_cqc_provider_df(cqc_provider_df)
    utils.write_to_parquet(
        cleaned_cqc_provider_df,
        cleaned_cqc_destination,
        append=True,
        partitionKeys=["year", "month", "day", "import_date"],
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
