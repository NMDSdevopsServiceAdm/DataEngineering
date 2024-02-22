import sys
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from utils import utils

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    MergeIndCqcColumns,
    MergeIndCqcValues,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    destination: str,
):
    cqc_location_df = utils.read_from_parquet(cleaned_cqc_location_source)
    cqc_pir_df = utils.read_from_parquet(cleaned_cqc_pir_source)
    ascwds_workplace_df = utils.read_from_parquet(cleaned_ascwds_workplace_source)

    ind_cqc_location_df = filter_df_to_independent_sector_only(cqc_location_df)

    utils.write_to_parquet(
        ind_cqc_location_df,
        destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def filter_df_to_independent_sector_only(df: DataFrame) -> DataFrame:
    return df.where(F.col(MergeIndCqcColumns.sector) == MergeIndCqcValues.independent)


if __name__ == "__main__":
    print("Spark job 'merge_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--cleaned_cqc_pir_source",
            "Source s3 directory for parquet CQC pir cleaned dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for parquet ASCWDS workplace cleaned dataset",
        ),
        (
            "--destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        ons_postcode_directory_source,
        destination,
    )

    print("Spark job 'merge_ind_cqc_data' complete")
