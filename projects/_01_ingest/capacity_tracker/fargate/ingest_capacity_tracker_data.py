import polars as pl

from polars_utils import utils
from utils import file_utils

DELIMITER = ","


def main(source: str, destination_prefix: str) -> None:
    """
    Ingest a single raw Capacity Tracker CSV extract into parquet.

    Args:
        source (str): A single CSV file in S3 to ingest.
        destination_prefix (str): Destination S3 directory for the ingested parquet.
    """
    _, key = file_utils.split_s3_uri(source)
    output_path = file_utils.construct_destination_path(destination_prefix, key) + "/"

    print(f"Reading CSV from {source} and writing to {output_path}")
    lf = pl.scan_csv(source, separator=DELIMITER, infer_schema=False)
    lf = sanitise_column_names(lf)

    utils.sink_to_parquet(lf, output_path)


def sanitise_column_names(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Replace invalid characters in column names and lowercase them.

    Raw extract headers are PascalCase (e.g. "CqcId"), but capacity_tracker_columns
    names them all lowercase with no word separators (e.g. "cqcid"), so columns are
    lowercased here to match rather than converted to snake_case.

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
            "--destination_prefix",
            "Destination s3 directory for capacity tracker data",
        ),
    )
    main(args.source, args.destination_prefix)
