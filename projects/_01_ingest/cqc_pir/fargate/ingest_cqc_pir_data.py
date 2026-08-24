import sys

import polars as pl

from polars_utils import utils
from utils import file_utils
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols

PIR_SCHEMA = pl.Schema(
    [
        (PIRCols.location_id, pl.String),
        (PIRCols.location_name, pl.String),
        (PIRCols.pir_type, pl.String),
        (PIRCols.pir_submission_date, pl.String),
        (PIRCols.pir_people_directly_employed, pl.Int32),
        (PIRCols.staff_leavers, pl.Int32),
        (PIRCols.staff_vacancies, pl.Int32),
        (PIRCols.shared_lives_leavers, pl.Int32),
        (PIRCols.shared_lives_vacancies, pl.Int32),
        (PIRCols.primary_inspection_category, pl.String),
        (PIRCols.region, pl.String),
        (PIRCols.local_authority, pl.String),
        (PIRCols.number_of_beds, pl.Int32),
        (PIRCols.domiciliary_care, pl.String),
        (PIRCols.location_status, pl.String),
    ]
)


def main(source: str, destination_prefix: str) -> None:
    """Ingests a single raw CQC PIR CSV file and sinks it to parquet.

    Args:
        source (str): the S3 URI of the raw PIR CSV file to ingest.
        destination_prefix (str): an S3 URI naming the destination bucket,
            with a trailing slash (e.g. "s3://bucket/"); the output path
            mirrors the source key's directory within it.
    """
    _, key = file_utils.split_s3_uri(source)
    output_path = file_utils.construct_destination_path(destination_prefix, key) + "/"

    print(f"Reading CSV from {source} with schema: {PIR_SCHEMA}")
    # PIR submissions arrive as Excel-exported CSVs, which are frequently not
    # valid UTF-8 (e.g. curly quotes/en-dashes in free-text fields encoded as
    # Windows-1252) - utf8-lossy replaces invalid byte sequences rather than
    # raising, matching the old PySpark job's more permissive CSV parsing.
    pir_lf = pl.scan_csv(source, schema=PIR_SCHEMA, encoding="utf8-lossy")

    print(f"Sinking parquet to {output_path}")
    utils.sink_to_parquet(lazy_df=pir_lf, output_path=output_path)


if __name__ == "__main__":
    print(f"Fargate job 'ingest_cqc_pir_data' called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--source", "A CSV file used as job input"),
        ("--destination_prefix", "A destination bucket for outputting parquet files"),
    )

    main(args.source, args.destination_prefix)
    print("Fargate job 'ingest_cqc_pir_data' complete")
