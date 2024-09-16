import sys

from pyspark.sql.dataframe import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

CAPACITY_TRACKER_NON_RES_COLUMNS = [
    CTNR.cqc_id,
    CTNR.nurses_employed,
    CTNR.care_workers_employed,
    CTNR.non_care_workers_employed,
    CTNR.agency_nurses_employed,
    CTNR.agency_care_workers_employed,
    CTNR.agency_non_care_workers_employed,
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
        capacity_tracker_care_home_source, CAPACITY_TRACKER_NON_RES_COLUMNS
    )
    columns_to_cast_to_integers = [
        CTNR.nurses_employed,
        CTNR.care_workers_employed,
        CTNR.non_care_workers_employed,
        CTNR.agency_nurses_employed,
        CTNR.agency_care_workers_employed,
        CTNR.agency_non_care_workers_employed,
    ]
    capacity_tracker_care_home_df = cUtils.cast_to_int(
        capacity_tracker_care_home_df, columns_to_cast_to_integers
    )
    capacity_tracker_care_home_df = cUtils.column_to_date(
        capacity_tracker_care_home_df,
        Keys.import_date,
        CTNRClean.capacity_tracker_import_date,
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
