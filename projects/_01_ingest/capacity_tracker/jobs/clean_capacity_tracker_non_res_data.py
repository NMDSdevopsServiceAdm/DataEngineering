import sys

from pyspark.sql import functions as F

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.capacity_tracker_columns import (
    CapacityTrackerNonResColumns as CTNR,
    CapacityTrackerNonResCleanColumns as CTNRClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

CAPACITY_TRACKER_NON_RES_COLUMNS = [
    CTNR.cqc_id,
    CTNR.cqc_care_workers_employed,
    CTNR.service_user_count,
    Keys.year,
    Keys.month,
    Keys.day,
    Keys.import_date,
]
MAX_BOUND: int = 3000
MIN_BOUND: int = 1


def main(
    capacity_tracker_non_res_source: str,
    cleaned_capacity_tracker_non_res_destination: str,
):
    capacity_tracker_non_res_df = utils.read_from_parquet(
        capacity_tracker_non_res_source, CAPACITY_TRACKER_NON_RES_COLUMNS
    )
    columns_to_cast_to_integers = [
        CTNR.cqc_care_workers_employed,
        CTNR.service_user_count,
    ]
    capacity_tracker_non_res_df = cUtils.cast_to_int(
        capacity_tracker_non_res_df, columns_to_cast_to_integers
    )
    capacity_tracker_non_res_df = cUtils.column_to_date(
        capacity_tracker_non_res_df,
        Keys.import_date,
        CTNRClean.ct_non_res_import_date,
    )
    columns_to_bound = [CTNR.cqc_care_workers_employed, CTNR.service_user_count]
    capacity_tracker_non_res_df = cUtils.set_bounds_for_columns(
        capacity_tracker_non_res_df,
        columns_to_bound,
        columns_to_bound,
        lower_limit=MIN_BOUND,
        upper_limit=MAX_BOUND,
    )
    capacity_tracker_non_res_df = capacity_tracker_non_res_df.withColumn(
        CTNRClean.care_home, F.lit(CareHome.not_care_home)
    )

    print(f"Exporting as parquet to {cleaned_capacity_tracker_non_res_destination}")
    utils.write_to_parquet(
        capacity_tracker_non_res_df,
        cleaned_capacity_tracker_non_res_destination,
        mode="overwrite",
        partitionKeys=[
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        ],
    )


if __name__ == "__main__":
    print("Spark job 'clean_capacity_tracker_non_res_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        capacity_tracker_non_res_source,
        cleaned_capacity_tracker_non_res_destination,
    ) = utils.collect_arguments(
        (
            "--capacity_tracker_non_res_source",
            "Source s3 directory for parquet capacity tracker non residential dataset",
        ),
        (
            "--cleaned_capacity_tracker_non_res_destination",
            "Destination s3 directory for cleaned parquet capacity tracker non residential dataset",
        ),
    )
    main(
        capacity_tracker_non_res_source,
        cleaned_capacity_tracker_non_res_destination,
    )

    print("Spark job 'clean_capacity_tracker_non_res_dataset' complete")
