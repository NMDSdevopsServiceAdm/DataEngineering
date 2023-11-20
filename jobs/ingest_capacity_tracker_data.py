import sys
import argparse
import pyspark.sql.functions as F

from schemas.capacity_tracker_schema import (
    CAPACITY_TRACKER_CARE_HOMES, 
    CAPACITY_TRACKER_NON_RESIDENTIAL,
    )
from utils import utils


def main(care_home_source, care_home_destination, non_res_source, non_res_destination):
    df = utils.read_csv_with_defined_schema(source, SPSS_JOBS_ESTIMATES)
    df_with_formatted_date = df.withColumn(
        "snapshot_date_formatted", F.col("Snapshot_date")
    )
    df_with_formatted_date = utils.format_date_fields(
        df_with_formatted_date,
        raw_date_format="yyyy/MM/dd",
        date_column_identifier="snapshot_date_formatted",
    )
    utils.write_to_parquet(df_with_formatted_date, destination, False)


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", help="A CSV file used as source input", required=True
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'ingest_capacity_tracker_data' starting...")
    print(f"Job parameters: {sys.argv}")

    care_home_source, care_home_destination, non_res_source, non_res_destination = collect_arguments()
    main(care_home_source, care_home_destination, non_res_source, non_res_destination)

    print("Spark job 'ingest_capacity_tracker_data' done")
