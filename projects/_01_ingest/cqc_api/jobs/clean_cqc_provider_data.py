import os
import sys

os.environ["SPARK_VERSION"] = "3.5"

import utils.cleaning_utils as cUtils
from utils import utils
from utils.column_names.cleaned_data_files.cqc_provider_cleaned import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cols_to_import = [
    CQCPClean.provider_id,
    CQCPClean.name,
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
]


def main(cqc_source: str, cleaned_cqc_destination: str):
    cqc_provider_df = utils.read_from_parquet(
        cqc_source, selected_columns=cols_to_import
    )

    cqc_provider_df = cUtils.column_to_date(
        cqc_provider_df, Keys.import_date, CQCPClean.cqc_provider_import_date
    )

    utils.write_to_parquet(
        cqc_provider_df,
        cleaned_cqc_destination,
        mode="overwrite",
        partitionKeys=cqcPartitionKeys,
    )


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
