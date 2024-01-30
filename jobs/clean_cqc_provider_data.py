import sys

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

# Test.
# Hi Roy!


def main(cqc_source: str, cleaned_cqc_destination: str):
    cqc_provider_df = utils.read_from_parquet(cqc_source)
    utils.write_to_parquet(
        cqc_provider_df,
        cleaned_cqc_destination,
        append=True,
        partitionKeys=cqcPartitionKeys,
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
