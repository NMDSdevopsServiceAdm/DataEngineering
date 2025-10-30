from pathlib import Path

import polars as pl

from polars_utils import logger, utils
from schemas.cqc_locations_delta_flattened_schema import LOCATIONS_FLATTENED_SCHEMA
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

logger = logger.get_logger(__name__)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    delta_flattened_source: str,
    full_flattened_destination: str,
) -> None:
    """
    Builds a full flattened CQC locations dataset from delta files.

    Only processes import_dates not already present in the destination.

    Args:
        delta_flattened_source (str): S3 URI to read delta flattened CQC locations data from
        full_flattened_destination (str): S3 URI to save full flattened CQC locations data to
    """

    # Scan delta flattened data in LazyFrame format
    delta_lf = utils.scan_parquet(delta_flattened_source)
    # schema=LOCATIONS_FLATTENED_SCHEMA, # TODO - update and include schema once finalised
    logger.info("CQC Location delta flattened LazyFrame read in")

    # TEMP - re-name lf but don't do anything
    full_lf = delta_lf

    # Identify unique import_dates in source delta data, sorted in order
    source_import_dates = (
        delta_lf.select(Keys.import_date)
        .unique()
        .sort(Keys.import_date)
        .collect()
        .to_series()
        .to_list()
    )
    logger.info(f"Found {len(source_import_dates)} import_dates in delta data source.")

    # Identify import_dates already present in destination
    dest_import_dates = utils.list_s3_parquet_import_dates(full_flattened_destination)
    logger.info(f"Found {len(dest_import_dates)} import_dates already in destination.")

    # Determine which import_dates to process, if any
    import_dates_to_process = [
        d for d in source_import_dates if d not in dest_import_dates
    ]

    if not import_dates_to_process:
        logger.info("No new import_dates to process. Exiting.")
        return

    logger.info(
        f"Processing {len(import_dates_to_process)} new import_dates: {import_dates_to_process}"
    )

    # Loop through new import_dates in date order

    # Store flattened data in s3 - TODO - include in loop
    utils.sink_to_parquet(
        full_lf,
        full_flattened_destination,
        logger=logger,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    logger.info("Running Full Flattened CQC Locations job")

    args = utils.get_args(
        (
            "--delta_flattened_source",
            "S3 URI to read delta flattened CQC locations data from",
        ),
        (
            "--full_flattened_destination",
            "S3 URI to save full flattened CQC locations data to",
        ),
    )

    main(
        delta_flattened_source=args.delta_flattened_source,
        full_flattened_destination=args.full_flattened_destination,
    )

    logger.info("Finished Full Flattened CQC Locations job")
