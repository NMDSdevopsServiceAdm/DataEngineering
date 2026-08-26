import sys

import polars as pl

from polars_utils import utils
from utils import file_utils

DELIMITER = ","


def main(source: str, destination_prefix: str) -> None:
    """Ingests a single raw ONS postcode directory CSV file and sinks it to parquet.

    ONS postcode directory files are always comma-delimited and read with no
    schema (every column as a string), matching the source data's existing
    lack of typed columns.

    Args:
        source (str): the S3 URI of the raw ONS CSV file to ingest.
        destination_prefix (str): an S3 URI naming the destination bucket,
            with a trailing slash (e.g. "s3://bucket/"); the output path
            mirrors the source key's directory within it.
    """
    _, key = file_utils.split_s3_uri(source)
    output_path = file_utils.construct_destination_path(destination_prefix, key) + "/"

    print(f"Reading comma-delimited CSV from {source} with all columns as strings")
    ons_lf = pl.scan_csv(source, separator=DELIMITER, infer_schema=False)

    print(f"Sinking parquet to {output_path}")
    utils.sink_to_parquet(lazy_df=ons_lf, output_path=output_path)


if __name__ == "__main__":
    print(f"Fargate job 'ingest_ons_data' called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--source", "A CSV file used as job input"),
        ("--destination_prefix", "A destination bucket for outputting parquet files"),
    )

    main(args.source, args.destination_prefix)
    print("Fargate job 'ingest_ons_data' complete")
