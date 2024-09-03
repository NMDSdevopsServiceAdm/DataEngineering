import sys

from pyspark.sql.dataframe import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


def main(
    capacity_tracker_care_home_source: str,
    cleaned_capacity_tracker_care_home_destination: str,
):
    capacity_tracker_care_home_df = utils.read_from_parquet(
        capacity_tracker_care_home_source,
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