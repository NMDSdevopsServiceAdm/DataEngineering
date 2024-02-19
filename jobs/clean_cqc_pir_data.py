import sys

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

from utils.column_names.cleaned_data_files.cqc_pir_cleaned_values import (
    CqcPIRCleanedColumns as PIRCleanCols,
    CqcPIRCleanedValues as PIRCleanValues,
)

pirPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(cqc_pir_source: str, cleaned_cqc_pir_destination: str):
    cqc_pir_df = utils.read_from_parquet(cqc_pir_source)

    cqc_pir_df = utils.remove_already_cleaned_data(
        cqc_pir_df, cleaned_cqc_pir_destination
    )

    cqc_pir_df = cUtils.column_to_date(
        cqc_pir_df, Keys.import_date, PIRCleanCols.cqc_pir_import_date
    )

    utils.write_to_parquet(
        cqc_pir_df,
        cleaned_cqc_pir_destination,
        mode="overwrite",
        partitionKeys=pirPartitionKeys,
    )


def add_care_home_column(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        PIRCleanCols.care_home,
        F.when(
            F.col(PIRCleanCols.pir_type) == PIRCleanValues.residential,
            PIRCleanValues.yes,
        ).otherwise(PIRCleanValues.no),
    )
    return df


if __name__ == "__main__":
    print("Spark job 'clean_cqc_pir_data' starting...")
    print(f"Job parameters: {sys.argv}")

    cqc_pir_source, cleaned_cqc_pir_destination = utils.collect_arguments(
        (
            "--cqc_pir_source",
            "Source s3 directory for parquet CQC providers dataset",
        ),
        (
            "--cleaned_cqc_pir_destination",
            "Destination s3 directory for cleaned parquet CQC providers dataset",
        ),
    )

    main(cqc_pir_source, cleaned_cqc_pir_destination)

    print("Spark job 'clean_cqc_pir_data' complete")
