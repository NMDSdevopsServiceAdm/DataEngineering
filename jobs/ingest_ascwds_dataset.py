import argparse
import sys

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


def main(source: str, destination: str, dataset: str = "ascwds"):
    """
    Main function to handle the ingestion of single or multiple ASCWDS/NMDSSC files.

    Args:
        source (str): The source CSV file or S3 directory.
        destination (str): The destination directory for outputting parquet files.
        dataset (str): The dataset type, either 'ascwds' or 'nmdssc'. Defaults to 'ascwds'.
    """
    bucket, prefix = utils.split_s3_uri(source)

    if utils.is_csv(source):
        ingest_single_file(source, bucket, prefix, destination, dataset)
    else:
        ingest_multiple_files(bucket, prefix, destination, dataset)


def ingest_single_file(
    source: str, bucket: str, prefix: str, destination: str, dataset: str
):
    """
    Handle the ingestion of a single CSV file.

    Args:
        source (str): The source file.
        bucket (str): The S3 bucket name.
        prefix (str): The S3 prefix (key) of the file.
        destination (str): The destination directory for outputting parquet files.
        dataset (str): The dataset type, either 'ascwds' or 'nmdssc'.
    """
    print("Single file provided to job. Handling single file.")
    new_destination = utils.construct_destination_path(destination, prefix)
    handle_job(source, bucket, prefix, new_destination, dataset)


def ingest_multiple_files(bucket: str, prefix: str, destination: str, dataset: str):
    """
    Handle the ingestion of multiple files.

    Args:
        bucket (str): The S3 bucket name.
        prefix (str): The S3 prefix (key) of the files.
        destination (str): The destination directory for outputting parquet files.
        dataset (str): The dataset type, either 'ascwds' or 'nmdssc'.
    """
    print("Multiple files provided to job. Handling each file...")
    objects_list = utils.get_s3_objects_list(bucket, prefix)

    print("Objects list:")
    print(objects_list)

    for key in objects_list:
        new_source = utils.construct_s3_uri(bucket, key)
        new_destination = utils.construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination, dataset)


def handle_job(
    source: str, source_bucket: str, source_key: str, destination: str, dataset: str
):
    """
    Handle the job of reading CSV file(s), processing then writing the data to parquet format.

    ASCWDS and NMDSSC files are very similar in structure but have some differences in the data they contain.
    - An error is raised if any new ASCWDS files contain unknown main job role IDs as this should no longer occur.
    - NMDSSC files contain date columns in the format MM/dd/yyyy which are converted to dd/MM/yyyy to match ASCWDS date formats.

    Args:
        source (str): The source file.
        source_bucket (str): The S3 bucket name.
        source_key (str): The S3 prefix (key) of the file.
        destination (str): The destination directory for outputting parquet files.
        dataset (str): The dataset type, either 'ascwds' or 'nmdssc'.
    """
    file_sample = utils.read_partial_csv_content(source_bucket, source_key)
    delimiter = utils.identify_csv_delimiter(file_sample)

    df = utils.read_csv(source, delimiter)
    if dataset == "ascwds":
        df = raise_error_if_mainjrid_includes_unknown_values(df)
    elif dataset == "nmdssc":
        df = fix_nmdssc_dates(df)
    else:
        raise ValueError("Error: dataset must be either 'ascwds' or 'nmdssc'")

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


def fix_nmdssc_dates(df: DataFrame) -> DataFrame:
    """
    Convert NMDS-SC date string format from MM/dd/yyyy to match the ASC-WDS string format of dd/MM/yyyy.

    Args:
        df (DataFrame): The DataFrame to adjust the date columns for.

    Returns:
        DataFrame: The DataFrame with the date columns adjusted.
    """
    DATE_COLUMN_SUFFIX = "date"
    date_columns = [column for column in df.columns if DATE_COLUMN_SUFFIX in column]

    for date_column in date_columns:
        df = df.withColumn(
            date_column,
            F.date_format(F.to_date(df[date_column], "MM/dd/yyyy"), "dd/MM/yyyy"),
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
    parser.add_argument(
        "--dataset",
        help="The dataset to be ingested (ascwds or nmdssc)",
        default="ascwds",
    )

    args, _ = parser.parse_known_args()

    return args.source, args.destination, args.dataset


if __name__ == "__main__":
    print("Spark job 'ingest_ascwds_dataset' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination, dataset = collect_arguments()
    main(source, destination, dataset)

    print("Spark job 'ingest_ascwds_dataset' complete")
