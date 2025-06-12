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
MAX_BOUND_DIRECTLY_EMPLOYED: int = 1000


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
    capacity_tracker_care_home_df = cUtils.cast_to_int(
        capacity_tracker_care_home_df, columns_to_cast_to_integers
    )
    capacity_tracker_care_home_df = cUtils.column_to_date(
        capacity_tracker_care_home_df,
        Keys.import_date,
        CTCHClean.capacity_tracker_import_date,
    )
    capacity_tracker_care_home_df = (
        remove_rows_where_agency_and_non_agency_values_match(
            capacity_tracker_care_home_df
        )
    )
    columns_to_bound = [
        CTCH.nurses_employed,
        CTCH.care_workers_employed,
        CTCH.non_care_workers_employed,
    ]
    capacity_tracker_care_home_df = cUtils.set_bounds_for_columns(
        capacity_tracker_care_home_df,
        columns_to_bound,
        columns_to_bound,
        upper_limit=MAX_BOUND_DIRECTLY_EMPLOYED,
    )
    capacity_tracker_care_home_df = create_new_columns_with_totals(
        capacity_tracker_care_home_df
    )

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
    Remove rows where the number of employed for non-agency job roles matches the corresponding agency job roles.

    Matching rows are removed as the likelihood of these numbers exactly matching is low and suggests that data quality may be poor.

    Args:
        df(DataFrame): A dataframe with capacity tracker care home data.

    Returns:
        DataFrame: A dataframe with suspect rows removed.
    """
    df = df.where(
        (df[CTCH.nurses_employed] != df[CTCH.agency_nurses_employed])
        | (df[CTCH.care_workers_employed] != df[CTCH.agency_care_workers_employed])
        | (
            df[CTCH.non_care_workers_employed]
            != df[CTCH.agency_non_care_workers_employed]
        )
    )
    return df


def create_new_columns_with_totals(df: DataFrame) -> DataFrame:
    """
    Adds new columns with totals for agency staff, non agency staff, and combined agency and non agency staff.

    Args:
        df(DataFrame): A dataframe with capacity tracker care home data.

    Returns:
        DataFrame: A dataframe with three additional columns with totals for agency staff, non agency staff, and combined agency and non agency staff.
    """
    df = df.withColumn(
        CTCHClean.non_agency_total_employed,
        df[CTCH.nurses_employed]
        + df[CTCH.care_workers_employed]
        + df[CTCH.non_care_workers_employed],
    )
    df = df.withColumn(
        CTCHClean.agency_total_employed,
        df[CTCH.agency_nurses_employed]
        + df[CTCH.agency_care_workers_employed]
        + df[CTCH.agency_non_care_workers_employed],
    )
    df = df.withColumn(
        CTCHClean.agency_and_non_agency_total_employed,
        df[CTCHClean.agency_total_employed] + df[CTCHClean.non_agency_total_employed],
    )
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
