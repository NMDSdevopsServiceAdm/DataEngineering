import sys

from pyspark.sql.dataframe import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerCareHomeColumns as CTCH,
    CapacityTrackerCareHomeCleanColumns as CTCHClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

CAPACITY_TRACKER_CARE_HOME_COLUMNS = [
    CTCH.cqc_id,
    CTCH.nurses_employed,
    CTCH.care_workers_employed,
    CTCH.non_care_workers_employed,
    CTCH.agency_nurses_employed,
    CTCH.agency_care_workers_employed,
    CTCH.agency_non_care_workers_employed,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]


def main(
    capacity_tracker_care_home_source: str,
    cleaned_capacity_tracker_care_home_destination: str,
):
    capacity_tracker_care_home_df = utils.read_from_parquet(
        capacity_tracker_care_home_source, CAPACITY_TRACKER_CARE_HOME_COLUMNS
    )
    columns_to_cast_to_integers = [
        CTCH.nurses_employed,
        CTCH.care_workers_employed,
        CTCH.non_care_workers_employed,
        CTCH.agency_nurses_employed,
        CTCH.agency_care_workers_employed,
        CTCH.agency_non_care_workers_employed,
    ]
    # change numbers to ints
    capacity_tracker_care_home_df = cUtils.cast_to_int(
        capacity_tracker_care_home_df, columns_to_cast_to_integers
    )
    # add import date column
    capacity_tracker_care_home_df = cUtils.column_to_date(
        capacity_tracker_care_home_df,
        Keys.import_date,
        CTCHClean.capacity_tracker_import_date,
    )
    # Remove those with exactly same directly employed and non-directly employed
    capacity_tracker_care_home_df = (
        remove_rows_where_agency_and_non_agency_values_match(
            capacity_tracker_care_home_df
        )
    )
    # add total agency column
    # add total non-agency column
    # add total agency and non agency column

    print(f"Exporting as parquet to {cleaned_capacity_tracker_care_home_destination}")
    utils.write_to_parquet(
        capacity_tracker_care_home_df,
        cleaned_capacity_tracker_care_home_destination,
        mode="overwrite",
        partitionKeys=[
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        ],
    )


def remove_rows_where_agency_and_non_agency_values_match(df: DataFrame) -> DataFrame:
    """
    Remove rows where the number of employed for a non-agency job role matches the corresponding agency job role.

    Matching rows are removed as the likelihood of this number exactly matching is low and suggests that data quality may be poor.

    Args:
        df(DataFrame): A dataframe with capacity tracker care home data.

    Returns:
        DataFrame: A dataframe with suspect rows removed.
    """
    return df


if __name__ == "__main__":
    print("Spark job 'clean_capacity_tracker_care_home_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        capacity_tracker_care_home_source,
        cleaned_capacity_tracker_care_home_destination,
    ) = utils.collect_arguments(
        (
            "--capacity_tracker_care_home_source",
            "Source s3 directory for parquet capacity tracker care home dataset",
        ),
        (
            "--cleaned_capacity_tracker_care_home_destination",
            "Destination s3 directory for cleaned parquet capacity tracker care home dataset",
        ),
    )
    main(
        capacity_tracker_care_home_source,
        cleaned_capacity_tracker_care_home_destination,
    )

    print("Spark job 'clean_capacity_tracker_care_home_dataset' complete")
