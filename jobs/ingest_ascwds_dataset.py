import argparse
import sys

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


def main(source: str, destination: str):
    bucket, prefix = utils.split_s3_uri(source)

    if utils.is_csv(source):
        ingest_single_file(source, bucket, prefix, destination)
    else:
        ingest_multiple_files(bucket, prefix, destination)


def ingest_single_file(source: str, bucket: str, prefix: str, destination: str):
    print("Single file provided to job. Handling single file.")
    new_destination = utils.construct_destination_path(destination, prefix)
    handle_job(source, bucket, prefix, new_destination)


def ingest_multiple_files(bucket: str, prefix: str, destination: str):
    print("Multiple files provided to job. Handling each file...")
    objects_list = utils.get_s3_objects_list(bucket, prefix)

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = utils.construct_s3_uri(bucket, key)
        new_destination = utils.construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination)


def handle_job(source: str, source_bucket: str, source_key: str, destination: str):
    file_sample = utils.read_partial_csv_content(source_bucket, source_key)
    delimiter = utils.identify_csv_delimiter(file_sample)

    df = utils.read_csv(source, delimiter)
    df = raise_error_if_mainjrid_includes_unknown_values(df)

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(df, destination)


def raise_error_if_mainjrid_includes_unknown_values(df: DataFrame) -> DataFrame:
    """
    Checks for unknown main job role IDs in the DataFrame and raises an error if any are found.

    This function runs for both workplace and worker files so the function first checks that the file contains the main job role column.
    If it does (worker file), it checks for unknown main job role IDs in the DataFrame and raises an error if any are found.

    Args:
        df (DataFrame): The DataFrame to check for unknown main job role IDs.

    Returns:
        DataFrame: The original DataFrame if no unknown values are found.

    Raises:
        ValueError: If the DataFrame contains unknown main job role IDs.
    """
    if AWK.main_job_role_id in df.columns:
        unknown_records = df.filter(F.col(AWK.main_job_role_id) == "-1")
        count_unknown = unknown_records.count()

        if count_unknown > 0:
            raise ValueError(
                f"Error: this file contains {count_unknown} unknown mainjrid record(s)"
            )

    return df


def collect_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        help="A CSV file or directory of files used as job input",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting parquet files",
        required=True,
    )

    args, _ = parser.parse_known_args()

    return args.source, args.destination


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = collect_arguments()
    main(source, destination)

    print("Spark job 'ingest_ascwds_dataset' complete")
