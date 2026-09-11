import sys

import polars as pl

from polars_utils import utils
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DPR,
)

SURVEY_SCHEMA = pl.Schema(
    [
        (DPR.YEAR, pl.Int32),
        (DPR.TOTAL_STAFF_RECODED, pl.Float32),
    ]
)

EXTERNAL_SCHEMA = pl.Schema(
    [
        (DPR.SERVICE_USER_DPRS_DURING_YEAR, pl.Float32),
        (DPR.SERVICE_USER_DPRS_AT_YEAR_END, pl.Float32),
        (DPR.CARER_DPRS_AT_YEAR_END, pl.Float32),
        (DPR.LA_AREA, pl.String),
        (DPR.DPRS_ADASS, pl.Float32),
        (DPR.DPRS_EMPLOYING_STAFF_ADASS, pl.Float32),
        (DPR.YEAR, pl.Int32),
        (DPR.PROPORTION_IMPORTED, pl.Float32),
        (DPR.HISTORIC_SERVICE_USERS_EMPLOYING_STAFF_ESTIMATE, pl.Float32),
        (DPR.FILLED_POSTS_PER_EMPLOYER, pl.Float32),
    ]
)


def main(source: str, dataset: str, destination: str) -> None:
    """Ingests a raw DPR CSV file and sinks it to parquet.

    Args:
        source (str): the S3 URI of the raw DPR CSV file to ingest.
        dataset (str): which DPR dataset `source` contains - either "survey" or
            "external" - used to select the schema to apply.
        destination (str): the S3 URI to sink the parquet output to, with a
            trailing slash (e.g. "s3://bucket/domain=01_dpr/dataset=survey/").

    Raises:
        ValueError: if `dataset` is neither "survey" nor "external".
    """
    match dataset:
        case "survey":
            schema = SURVEY_SCHEMA
        case "external":
            schema = EXTERNAL_SCHEMA
        case _:
            raise ValueError(
                f"Unknown dataset '{dataset}'. Must be either 'survey' or 'external'."
            )

    print(f"Reading CSV from {source} with schema: {schema}")
    # DPR CSVs are Excel-exported and frequently not valid UTF-8; utf8-lossy avoids failing on that.
    dpr_lf = pl.scan_csv(source, schema=schema, encoding="utf8-lossy")

    print(f"Sinking parquet to {destination}")
    utils.sink_to_parquet(lazy_df=dpr_lf, output_path=destination)


if __name__ == "__main__":
    print(f"Fargate job 'ingest_dpr_data' called with parameters: {sys.argv}")

    args = utils.get_args(
        ("--source", "A CSV file used as job input"),
        ("--dataset", "Which DPR dataset is being ingested: 'survey' or 'external'"),
        ("--destination", "A destination directory for outputting parquet files"),
    )

    main(args.source, args.dataset, args.destination)
    print("Fargate job 'ingest_dpr_data' complete")
