import sys

import pyspark.sql.dataframe

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


def clean_cqc_location_df(df_to_clean) -> pyspark.sql.DataFrame:
    # TODO Cleaning logic
    return df_to_clean


def main(cqc_location_source: str, cleaned_cqc_location_destintion: str):
    cqc_location_df = utils.read_from_parquet(cqc_location_source)
    print(cqc_location_df)
    cleaned_cqc_location_df = clean_cqc_location_df(cqc_location_df)
    utils.write_to_parquet(
        cleaned_cqc_location_df,
        cleaned_cqc_location_destintion,
        append=True,
        partitionKeys=[Keys.year, Keys.month, Keys.day, Keys.import_date],
    )


if __name__ == "__main__":
    print("Spark job 'clean_cqc_location_data' starting...")
    print(f"Job parameters: {sys.argv}")

    cqc_location_source, cleaned_cqc_location_destination = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--cleaned_cqc_location_destination",
            "Destination s3 directory for cleaned parquet CQC locations dataset",
        ),
    )
    main(cqc_location_source, cleaned_cqc_location_destination)

    print("Spark job 'clean_cqc_location_data' complete")
