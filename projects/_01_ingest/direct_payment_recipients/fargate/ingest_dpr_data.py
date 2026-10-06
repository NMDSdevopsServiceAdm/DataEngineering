import sys

import polars as pl

from polars_utils import utils
from utils.column_names.direct_payments_column_names import (
    DirectPaymentColumnNames as DPR,
)

SURVEY_SCHEMA = pl.Schema(
    [
        (DPR.year, pl.Int32),
        (DPR.total_staff_recoded, pl.Float32),
    ]
)

EXTERNAL_SCHEMA = pl.Schema(
    [
        (DPR.service_user_dprs_during_year, pl.Float32),
        (DPR.service_user_dprs_at_year_end, pl.Float32),
        (DPR.carer_dprs_at_year_end, pl.Float32),
        (DPR.la_area, pl.String),
        (DPR.dprs_adass, pl.Float32),
        (DPR.dprs_employing_staff_adass, pl.Float32),
        (DPR.year, pl.Int32),
        (DPR.proportion_imported, pl.Float32),
        (DPR.historic_service_users_employing_staff_estimate, pl.Float32),
        (DPR.filled_posts_per_employer, pl.Float32),
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
