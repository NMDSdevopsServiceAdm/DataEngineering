import sys

import polars as pl

from polars_utils import utils
from utils import file_utils


def main(source: str, destination: str) -> None:
    """
    Ingest raw Capacity Tracker CSV extract(s) into parquet.

    Args:
        source (str): A single CSV file, or an S3 directory of CSV files, to ingest.
        destination (str): Destination S3 directory for the ingested parquet.
    """
    if file_utils.is_csv(source):
        print("Single file provided to job. Handling single file.")
        bucket, key = file_utils.split_s3_uri(source)
        new_destination = file_utils.construct_destination_path(destination, key)
        handle_job(source, bucket, key, new_destination)
        return

    print("Multiple files provided to job. Handling each file...")
    bucket, prefix = file_utils.split_s3_uri(source)
    objects_list = file_utils.get_s3_objects_list(bucket, prefix)

    for key in objects_list:
        new_source = file_utils.construct_s3_uri(bucket, key)
        new_destination = file_utils.construct_destination_path(destination, key)
        handle_job(new_source, bucket, key, new_destination)


def handle_job(
    source: str, source_bucket: str, source_key: str, destination: str
) -> None:
    """
    Detect a CSV file's delimiter and ingest it.

    Args:
        source (str): S3 path of the CSV file to ingest.
        source_bucket (str): S3 bucket containing the source file.
        source_key (str): S3 key of the source file, used to sample its content.
        destination (str): Destination S3 directory for the ingested parquet.
    """
    file_sample = file_utils.read_partial_csv_content(source_bucket, source_key)
    delimiter = file_utils.identify_csv_delimiter(file_sample)
    ingest_dataset(source, destination, delimiter)


def ingest_dataset(source: str, destination: str, delimiter: str) -> None:
    """
    Read a CSV with the given delimiter and sink it to parquet.

    Columns are read as strings (no schema inference) as this raw extract's column set
    and types vary between files; typing happens in the clean step.

    Args:
        source (str): S3 path of the CSV file to ingest.
        destination (str): Destination S3 directory for the ingested parquet.
        delimiter (str): The CSV field delimiter.
    """
    print(
        f"Reading CSV from {source} and writing to {destination} with delimiter: {delimiter}"
    )
    lf = pl.scan_csv(source, separator=delimiter, infer_schema=False)
    lf = sanitise_column_names(lf)

    utils.sink_to_parquet(lf, destination.rstrip("/") + "/")


def sanitise_column_names(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Replace invalid characters in column names with characters to match current file names.

    Args:
        lf (pl.LazyFrame): A LazyFrame with capacity tracker data.

    Returns:
        pl.LazyFrame: The input LazyFrame with invalid characters removed from column names.
    """
    rename_map = {
        column: column.replace(" ", "_").replace("(", "").replace(")", "")
        for column in lf.collect_schema().names()
    }
    return lf.rename(rename_map)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--source",
            "A CSV file or directory of csv files in s3 with capacity tracker data to import",
        ),
        (
            "--destination",
            "Destination s3 directory for capacity tracker data",
        ),
    )
    main(args.source, args.destination)
