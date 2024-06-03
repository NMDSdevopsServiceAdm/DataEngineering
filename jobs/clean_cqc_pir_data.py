import sys

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRCleanCols,
    CqcPIRCleanedValues as PIRCleanValues,
)


pirPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(cqc_pir_source: str, cleaned_cqc_pir_destination: str):
    cqc_pir_df = utils.read_from_parquet(cqc_pir_source)

    cqc_pir_df = cUtils.column_to_date(
        cqc_pir_df, Keys.import_date, PIRCleanCols.cqc_pir_import_date
    )
    cqc_pir_df = cUtils.column_to_date(
        cqc_pir_df,
        PIRCleanCols.pir_submission_date,
        PIRCleanCols.pir_submission_date_as_date,
        cUtils.pir_submission_date_uri_format,
    )

    cqc_pir_df = add_care_home_column(cqc_pir_df)

    cqc_pir_df = filter_latest_submission_date(cqc_pir_df)

    cqc_pir_df = cqc_pir_df.withColumnRenamed(
        PIRCols.people_directly_employed, PIRCleanCols.people_directly_employed
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
        )
        .when(
            F.col(PIRCleanCols.pir_type) == PIRCleanValues.community,
            PIRCleanValues.no,
        )
        .otherwise(None),
    )
    return df


def filter_latest_submission_date(df: DataFrame) -> DataFrame:
    """
    For a given cleaned cqc pir DataFrame that contains:
     - location_id (String)
     - cqc_pir_import_date (String[Date])
     - care_home (String)
     - pir_submission_date_as_date (String[Date])
    Filters the latest submission date per grouping of location, import date and care home status

    Args:
        df (DataFrame): A cqc pir dataframe that must contain at least the columns above

    Returns:
        single_row_per_grouping_df (DataFrame): A filtered form of the input df,
        where there is now only the latest submission date per grouping of the other 3 fields
    """

    latest_submission_date_per_grouping_df = utils.latest_datefield_for_grouping(
        df,
        [
            F.col(PIRCleanCols.location_id),
            F.col(PIRCleanCols.cqc_pir_import_date),
            F.col(PIRCleanCols.care_home),
        ],
        F.col(PIRCleanCols.pir_submission_date_as_date),
    )
    single_row_per_grouping_df = latest_submission_date_per_grouping_df.dropDuplicates(
        [
            PIRCleanCols.location_id,
            PIRCleanCols.cqc_pir_import_date,
            PIRCleanCols.care_home,
            PIRCleanCols.pir_submission_date_as_date,
        ]
    )
    return single_row_per_grouping_df


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
