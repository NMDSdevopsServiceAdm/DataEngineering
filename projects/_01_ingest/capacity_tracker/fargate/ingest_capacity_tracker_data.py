import sys

import polars as pl

from polars_utils import utils
from utils import file_utils


def main(source: str, destination: str) -> None:
    """
    Ingest a single raw Capacity Tracker CSV extract into parquet.

    Args:
        source (str): A single CSV file in S3 to ingest.
        destination (str): Destination S3 directory naming the target domain/dataset
            (e.g. "s3://bucket/domain=capacity_tracker/dataset=capacity_tracker_care_home_polars/").
            The source key's own partition path (year/month/day/import_date) is
            appended to it, so the clean step's Hive-partition discovery can pick up
            import_date the same way it does for the PySpark output.
    """
    bucket, key = file_utils.split_s3_uri(source)
    # TEMPORARY, while this job runs alongside the PySpark version for output
    # comparison: `destination`'s dataset name is "_polars"-suffixed, so it can't
    # reuse `file_utils.construct_destination_path` as-is (that keeps only the
    # bucket from `destination` and rebuilds the whole path from `key`, which
    # would reproduce the PySpark job's unsuffixed path and collide with it).
    # Once the PySpark version is retired and `destination` no longer needs to
    # diverge from the source's own dataset name, delete `partition_path_from_key`
    # and replace the next line with:
    #   new_destination = file_utils.construct_destination_path(destination, key)
    new_destination = destination.rstrip("/") + "/" + partition_path_from_key(key)

    file_sample = file_utils.read_partial_csv_content(bucket, key)
    delimiter = file_utils.identify_csv_delimiter(file_sample)

    ingest_dataset(source, new_destination, delimiter)


def partition_path_from_key(key: str) -> str:
    """
    Extract the partition path following the 'dataset=' segment of a raw S3 key.

    TEMPORARY, see the comment in `main` — delete this once the PySpark version
    is retired.

    The raw CSV has no import_date column of its own — it's only present as a
    Hive-style "import_date=YYYYMMDD" folder in the S3 key. Preserving that
    (and the other partition folders alongside it) in the output lets
    `polars_utils.utils.scan_parquet` auto-discover import_date as a column when
    the clean job reads it back, the same way Spark does for the PySpark output.

    Args:
        key (str): S3 key of a raw capacity tracker CSV, e.g.
            "domain=capacity_tracker/dataset=capacity_tracker_care_home/year=2026/month=08/day=01/import_date=20260801/file.csv".

    Returns:
        str: The partition path with a trailing slash, e.g.
            "year=2026/month=08/day=01/import_date=20260801/".
    """
    directory_parts = file_utils.get_file_directory(key).split("/")
    dataset_index = next(
        index
        for index, part in enumerate(directory_parts)
        if part.startswith("dataset=")
    )
    return "/".join(directory_parts[dataset_index + 1 :]) + "/"


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
