import sys

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
)
import pyspark.sql.functions as F

from utils import utils
import utils.cleaning_utils as cUtils

from utils.column_names.raw_data_files.ons_columns import (
    ONSPartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


onsPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(ons_source: str, cleaned_ons_destination: str):
    ons_df = utils.read_from_parquet(ons_source)

    ons_df = cUtils.column_to_date(ons_df, Keys.import_date, ONSClean.ons_import_date)

    utils.write_to_parquet(
        ons_df,
        cleaned_ons_destination,
        mode="overwrite",
        partitionKeys=onsPartitionKeys,
    )


if __name__ == "__main__":
    # Where we tell Glue how to run the file, and what to print out
    print("Spark job 'clean_ons_data' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--ons_source",
            "Source s3 directory for parquet ONS postcode directory dataset",
        ),
        (
            "--cleaned_ons_destination",
            "Destination s3 directory for cleaned parquet ONS postcode directory dataset",
        ),
    )
    # Python logic ---> all in main
    main(source, destination)

    print("Spark job 'clean_ons_data' complete")
