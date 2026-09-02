# TODO(1995): throwaway change to trigger this domain's raw-bucket reseed and verify
# Ingest-ASCWDS against the polars/pointblank bump. Remove before merging.
import sys

import polars as pl

from polars_utils import utils
from utils import file_utils
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)

DELIMITER = "|"


def main(source: str, destination_prefix: str) -> None:
    """Ingests a single raw ASCWDS workplace or worker CSV file and sinks it to parquet.

    ASCWDS raw files are always pipe-delimited and read with no schema (every
    column as a string) since workplace and worker files together span 800+
    columns that change frequently, and this job handles either file type
    generically without knowing in advance which one it's reading.

    Args:
        source (str): the S3 URI of the raw ASCWDS CSV file to ingest.
        destination_prefix (str): an S3 URI naming the destination bucket,
            with a trailing slash (e.g. "s3://bucket/"); the output path
            mirrors the source key's directory within it.
    """
    _, key = file_utils.split_s3_uri(source)
    output_path = file_utils.construct_destination_path(destination_prefix, key) + "/"

    print(f"Reading pipe-delimited CSV from {source} with all columns as strings")
    ascwds_lf = pl.scan_csv(source, separator=DELIMITER, infer_schema=False)

    raise_error_if_mainjrid_includes_unknown_values(ascwds_lf)

    print(f"Sinking parquet to {output_path}")
    utils.sink_to_parquet(lazy_df=ascwds_lf, output_path=output_path)


def raise_error_if_mainjrid_includes_unknown_values(lf: pl.LazyFrame) -> None:
    """Raises an error if a worker file's main job role column contains unknown values.

    This job handles both workplace and worker files, so it first checks
    whether the main job role column is present at all (only worker files
    have it). If present, any row with the sentinel unknown value "-1"
    should no longer occur in new files, so its presence is treated as a
    hard failure here.

    Args:
        lf (pl.LazyFrame): the scanned CSV data to check.

    Raises:
        ValueError: if the LazyFrame contains unknown main job role IDs.
    """
    if AWK.main_job_role_id not in lf.collect_schema().names():
        return

    # A small aggregation collect (a single count), not a full materialisation
    # of the LazyFrame - the source CSV is still read a second time here since
    # CSV has no columnar pruning, matching the two-pass cost the original
    # PySpark filter().count() had.
    count_unknown = (
        lf.select((pl.col(AWK.main_job_role_id) == "-1").sum()).collect().item()
    )

    if count_unknown > 0:
        raise ValueError(
            f"Error: this file contains {count_unknown} unknown mainjrid record(s)"
        )


if __name__ == "__main__":
    print(f"Fargate job 'ingest_ascwds_dataset' called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--source", "A CSV file used as job input"),
        ("--destination_prefix", "A destination bucket for outputting parquet files"),
    )

    main(args.source, args.destination_prefix)
    print("Fargate job 'ingest_ascwds_dataset' complete")
