import sys

import polars as pl

from polars_utils import utils
from utils import file_utils


def main(source: str, destination: str) -> None:
    """
    Ingest a single raw Capacity Tracker CSV extract into parquet.

    Args:
        source (str): A single CSV file in S3 to ingest.
        destination (str): Destination S3 directory for the ingested parquet.
    """
    bucket, key = file_utils.split_s3_uri(source)

    file_sample = file_utils.read_partial_csv_content(bucket, key)
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
    Replace invalid characters in column names and lowercase them.

    Raw extract headers are PascalCase (e.g. "CqcId"), but `utils.column_names.
    capacity_tracker_columns` names are all lowercase (e.g. "cqcid") to match what the
    legacy PySpark job produced under Spark's case-insensitive column resolution.
    Polars has no such case-insensitivity, so columns are lowercased here to match.

    Args:
        lf (pl.LazyFrame): A LazyFrame with capacity tracker data.

    Returns:
        pl.LazyFrame: The input LazyFrame with invalid characters removed from column
            names, and column names lowercased.
    """
    rename_map = {
        column: column.replace(" ", "_").replace("(", "").replace(")", "").lower()
        for column in lf.collect_schema().names()
    }
    return lf.rename(rename_map)


if __name__ == "__main__":
    args = utils.get_args(
        (
            "--source",
            "A single CSV file in s3 with capacity tracker data to import",
        ),
        (
            "--destination",
            "Destination s3 directory for capacity tracker data",
        ),
    )
    main(args.source, args.destination)
